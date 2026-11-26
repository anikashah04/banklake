"""Schema converter: maps legacy schema/table references to target (Athena/Glue) names."""
from dataclasses import dataclass, field

from .parser import SKIP, Tok, next_sig, prev_sig

DEFAULT_TABLE_MAP = {
    "legacy_db.customer": "banklake.silver_customer", "legacy_db.account": "banklake.silver_account",
    "legacy_db.transaction": "banklake.silver_transaction", "legacy_db.branch": "banklake.silver_branch",
    "legacy_db.deposit": "banklake.silver_deposit", "legacy_db.loan": "banklake.silver_loan",
    "banking.customer_master": "banklake.silver_customer", "banking.account_master": "banklake.silver_account",
    "banking.transaction_details": "banklake.silver_transaction", "banking.branch_master": "banklake.silver_branch",
    "banking.deposit_details": "banklake.silver_deposit", "banking.loan_details": "banklake.silver_loan",
}
INLINE_FROM = {"EXTRACT", "SUBSTRING", "TRIM", "OVERLAY"}
STOP = {"WHERE", "GROUP", "ORDER", "HAVING", "UNION", "INTERSECT", "EXCEPT", "MINUS", "QUALIFY", "LIMIT", "ON", "USING", "SELECT", "SEL"}


@dataclass
class SchemaMapper:
    table_map: dict = field(default_factory=lambda: dict(DEFAULT_TABLE_MAP))
    flatten_unknown: bool = True  # schema.table -> schema_table when no explicit mapping exists

    def map_table(self, ref: str):
        key = ref.lower()
        if key in self.table_map:
            return self.table_map[key], f"table {ref} -> {self.table_map[key]}"
        if "." in key and self.flatten_unknown:
            flat = key.replace(".", "_")
            return flat, f"table {ref} -> {flat} (no explicit mapping; schema flattened)"
        return ref, None

    def apply(self, tokens: list[Tok]):
        """Rewrite table references that follow FROM / JOIN (and comma-joined tables). Returns (tokens, notes)."""
        out, notes, i, n = list(tokens), [], 0, len(tokens)
        in_from, depth_from = False, 0
        depth = 0
        opened = []          # the word before each open bracket, to tell EXTRACT(x FROM col) from a real FROM clause
        while i < n:
            t = out[i]
            if t.kind == "op" and t.text == "(":
                depth += 1
                p = prev_sig(out, i)
                opened.append(out[p].text.upper() if p != -1 and out[p].kind == "word" else "")
            elif t.kind == "op" and t.text == ")":
                depth -= 1
                if opened:
                    opened.pop()
                if in_from and depth < depth_from:
                    in_from = False
            take = False
            if t.is_word("FROM") and opened and opened[-1] in INLINE_FROM:
                pass     # EXTRACT(YEAR FROM d.col), SUBSTRING(x FROM 2), TRIM(BOTH ' ' FROM x): FROM here is not a table list
            elif t.is_word("FROM", "JOIN"):
                in_from, depth_from, take = True, depth, True
            elif in_from and t.kind == "op" and t.text == "," and depth == depth_from:
                take = True
            elif t.is_word(*STOP) and depth == depth_from:
                in_from = False
            if take:
                j = next_sig(out, i)
                if j != -1 and out[j].kind == "word" and not out[j].is_word("SELECT", "SEL"):
                    chain, k = [out[j].text], j
                    while True:
                        d = next_sig(out, k)
                        w = next_sig(out, d) if d != -1 else -1
                        if d != -1 and out[d].text == "." and w != -1 and out[w].kind == "word":
                            chain.append(out[w].text)
                            k = w
                        else:
                            break
                    ref = ".".join(chain)
                    new, note = self.map_table(ref)
                    if note:
                        notes.append(note)
                        out[j:k + 1] = [Tok("raw", new)]
                        n = len(out)
            i += 1
        return out, notes
