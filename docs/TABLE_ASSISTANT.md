# Conversational table editing

## Workflow

1. Import your S3 CSV/XLSX file or load demo data.
2. Enter your desired result in **Chat with your table**, then select **Send instruction**. No transformation type or column picker is required.
3. AI considers the column names, table sample, current instruction, and recent conversation. It chooses columns and an ordered plan, including multiple steps when needed.
4. Clear requests produce a new table. Ambiguous or unsupported requests produce **Needs clarification**, leaving the current table unchanged. Answer in the same box.
5. Each successful edit becomes the basis for the next instruction. **Reset to original** resets both this basis and the conversation; **View original** changes only the preview.

## Examples

- Set every Notes value to test and trim Name.
- Keep only Name without changing its values.
- Replace example.com with test.org in Email.
- Keep rows where Price is greater than 100 and sort Price descending as numbers.
- Rename Name to CustomerName.
- Multiply Price by 0.9 and save it as DiscountedPrice.
- Fill empty Notes values with missing.

Requests such as “Change it” or “Improve this” require clarification. Supported operations are assignment, conditional assignment, replacement, extraction, case/space normalization, exact value mapping, blank filling, column selection/renaming, row filtering, constant arithmetic, concatenation, deduplication, and sorting. Arbitrary translation or semantic rewriting requires a reliable deterministic rule; it is never silently approximated as lowercasing.

AI explanations and clarification questions use English, including when the instruction is in another language. Source column labels and cell values remain unchanged unless the user asks to edit them.

## Data sent to AI

The configured provider (currently Groq) receives column names, total row count, the instruction, recent conversation, and **up to eight representative sample rows** from the current table. Samples come from multiple persisted pages; a small table may fit entirely.

Each cell is capped at 160 characters and serialized table context at approximately 6 KB. Long values are truncated. **View sample** shows the bounded context before submission. When the working dataset is unchanged, the backend uses the same deterministic sampling rules.

The full dataset is processed by local Spark. Samples do not prove the state of all rows, and AI must not claim it inspected the entire table. AWS and model configuration credentials are excluded from table context. Values already present in a table are part of the sample sent when you submit a request.

## Verification

```bash
# HTTP API, live model, and Spark with synthetic data
python3 scripts/chat_smoke_test.py --url http://localhost:8080

# Container-side synthetic AI check; does not require S3
docker compose exec worker python check_ai.py
```

These checks make real requests to your configured provider and consume its quota. Free-plan rate limits may require retries. The application does not switch to paid OpenAI as a fallback.
