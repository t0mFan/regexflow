# RegexFlow deployment demonstration

[Open the live application](https://regexflow.3.26.29.195.sslip.io/) · [Watch / download the MP4](regexflow-demo.mp4) · [English captions](captions.vtt)

[![RegexFlow demonstration preview](preview.jpg)](regexflow-demo.mp4)

Recorded on **18 October 2026**, using the deployed application in Chrome and the real `s3-test.csv` object in the user's S3 bucket. The CSV contains three synthetic records with `Name`, `Email`, and `Notes` columns.

## What the recording shows

1. The public HTTPS workbench and S3 connection dialog.
2. Real bucket file selection, asynchronous import progress, and paginated results.
3. Regex replacement of `example.com` with `demo.org` in email addresses.
4. A subsequent request to uppercase `Name` and extract the first word into `FirstName`, preserving the updated emails.
5. A request to categorize `Notes` without supplying category rules, explicitly asking the assistant to clarify before editing. The assistant asks for the missing rules and keeps the table unchanged.
6. Switching between the original three-column table and the edited four-column result.

The model also repeated the previous email-domain rule during the second edit. The application displayed a no-match warning because the domain had already changed. This behavior is retained and explained in the video; the requested name edits completed successfully. A vague follow-up such as “Change it” can be interpreted using prior conversation rather than trigger clarification; that exploratory request is omitted from the edited sequence. The clarification example above explicitly requests a question about missing rules.

## Recording and playback notes

- Actual browser captures are edited into a narrated tutorial. Waiting periods and an exploratory request are omitted; this is not an uninterrupted timing benchmark.
- AWS credential entry happened privately before recording. The connection dialog shown has empty credential fields. No API keys, AWS secret keys, or session cookies are included.
- English explanation captions are burned into the picture and supplied separately as WebVTT. The narration uses a local system text-to-speech voice.
- The MP4 uses H.264 video, AAC audio, and a fast-start layout. Open it in a browser or download it to a video player. GitHub's repository view may provide a download instead of an inline video player.
- This short demonstration uses three rows. It does not establish million-row end-to-end performance or exhaustive correctness of arbitrary AI instructions. Existing benchmark reports describe their separate scope.
- The S3 source object is not overwritten by these edits. AI requests send bounded table samples to the configured Groq provider, as described in the main README.

`chapters.json` contains chapter timings and the English narration transcript.
