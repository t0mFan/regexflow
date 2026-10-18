# RegexFlow: edit tables with one prompt

## Start locally

Open Docker Desktop, then run from this directory:

```bash
python3 scripts/configure.py
# Run only if the model key has not been configured. Input stays hidden.
python3 scripts/configure_groq.py
docker compose up --build -d
```

Open http://localhost:8080. An existing private `.env` is retained; you do not need to enter your key again. Never send keys in chat or commit them to source control.

## Use the workbench

Import a CSV/XLSX file from your S3 bucket or load demo data. In **Chat with your table**, enter a request such as:

- Set all Notes values to test and trim the Name column.
- Keep only the Name column.
- Replace example.com with test.org in Email.

You do not need to choose a transformation type, target columns, or a replacement field. AI receives column names, up to eight representative sample rows, and your instruction, then selects the editing steps. Click **View sample** before sending to inspect the sample. Spark processes the full table locally.

If your instruction is unclear, AI asks a question without changing the table. Reply in the same input box. Further edits use the latest successful result. **Reset to original** starts over; **View original** only changes the preview. Original S3 files and the imported source are preserved.

The interface, new AI explanations, task messages, and documentation use English. Input prompts and table contents may still contain other languages. Existing conversation records are retained as originally written.

See [Table assistant](docs/TABLE_ASSISTANT.md), [AI setup](docs/AI_SETUP.md), and [Architecture and limitations](README.md).
