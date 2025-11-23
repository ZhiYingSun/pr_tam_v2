# pr_tam_v2
rewrite of pr_tam

```
   cd /Users/sophysun/PycharmProjects/pr_tam_v2
   source .venv/bin/activate
   set -a && . ./.env.local && set +a
   ```

   ```
   python -m src.main
   ```

This reads `data/Puerto Rico Data_ v1109_50_without_LLC.csv`, filters/export the restaurants, hits Zyte/OpenAI to find matches, and drops CSVs under `data/` (filtered snapshots) plus `data/output/` (matched/unmatched) and a `final_output_<timestamp>.csv` in the project root.
