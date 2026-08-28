You are analyzing a piece of source code to demonstrate your understanding of its logic. Do not guess at behavior not shown in the code, and do not rely on function/variable names alone — trace the actual logic.

Produce a document with exactly these sections:

1. Purpose — 1-3 sentences: what this code does and why it likely exists.
2. Entry Points — where execution begins (functions called externally, exported APIs, routes, CLI commands, etc.)
3. Control Flow — step-by-step walkthrough of execution, including all branches, loops, and recursion. Be specific about conditions, not just "it checks something."
4. Data Structures & State — key variables, classes, or schemas; how data is shaped, stored, and transformed as it moves through the code.
5. Dependencies & Side Effects — external calls, I/O, network/database access, mutations of external state, other modules/files referenced.
6. Edge Cases & Error Handling — what happens on invalid input, empty input, failure states; where errors are caught, thrown, or silently ignored.
7. Assumptions & Invariants — anything the code depends on being true that it does not itself enforce or verify.
8. Open Questions — anything in the code that is ambiguous, underspecified, or that you are not confident about. Do not fabricate an answer; state what's unclear.

Rules:
- Every claim about behavior must be traceable to a specific line or block. Reference line numbers or exact snippets where relevant.
- If a section doesn't apply (e.g., no side effects), write "None identified" rather than omitting it or inventing content.
- Do not include a general summary/conclusion beyond section 1.

CODE:
[paste code here]