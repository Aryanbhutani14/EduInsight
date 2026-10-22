# EduInsight

**Hadoop-Based Large-Scale Lecture Analytics and Recommendation System** — a Big Data Analytics course project.

EduInsight analyses a large collection of lecture videos *collectively* (not one video at a time): it extracts topics from the captions, finds similar and redundant lectures, clusters them, and ranks lectures for a student's query by measurable content match (never "best teacher"). Storage is HDFS and processing is Apache Spark.

Dataset: [MOOCCubeX](https://github.com/THU-KEG/MOOCCubeX) (THU-KEG, Tsinghua) — 3,781 courses, 59,581 captioned lecture videos, about 10,543 hours, from the Chinese MOOC platform XuetangX. Captions are Chinese, so the pipeline uses jieba tokenization and (planned) a multilingual sentence-transformer so an English query can match Chinese lectures.

## Status

| Stage | Status |
|---|---|
| Dataset selection, verification, course↔video bridge, CS subset, EDA | Done |
| Module 1: HDFS + Spark environment, subset ingested | Done |
| Module 2: Spark preprocessing (jieba, stopwords, concept filtering, term statistics) | Done |
| Module 3: Multilingual embeddings | In progress |
| Module 4: Similarity and clustering | Planned |
| Module 5: Ranking and recommendation engine | Planned |
| Module 6: Integration and query demo | Planned |

## Results so far

- **Data verification:** the first `video.json` download was truncated (14 MB vs about 580 MB); re-downloaded and validated line by line (59,581 valid records). The official README omits the file that links course resources to video captions (`relations/video_id-ccid.txt`); it was found through the dataset's GitHub issue tracker and verified.
- **CS development subset:** 59 Computer Science courses with at least 10 captioned videos each, 4,071 videos, 590.9 hours, 27,552 concepts.
- **EDA finding:** 2.9% of concepts are 1–2 character generic words that dominate raw frequency rankings; they are filtered in Module 2.
- **Module 2 (Spark, 29 s on the subset):** 5.94M raw tokens reduced to 1.80M, 61,094-term vocabulary, 26,766 clean concepts, 3,938 of 4,071 videos linked to clean concepts. Outputs are Parquet files in HDFS under `/eduinsight/processed/`.

HDFS runs single-node (pseudo-distributed, replication 1) and Spark runs in `local[*]` mode on one machine.

## Repository layout

| Path | Contents |
|---|---|
| `scripts/build_cs_subset.py` | Builds the CS subset from the full dataset files |
| `scripts/eda_cs_subset.py`, `scripts/dashboard_stats.py` | EDA charts and corpus statistics |
| `scripts/wsl_*.sh`, `scripts/start_eduinsight_cluster.sh`, `scripts/stop_eduinsight_cluster.sh` | Hadoop/Spark setup, HDFS ingestion, cluster start/stop (WSL2 Ubuntu 22.04) |
| `scripts/spark_smoke_test.py` | Module 1 check: Spark reading from HDFS |
| `scripts/spark_module2_preprocess.py` | Module 2 Spark job |
| `eda/` | Generated charts and statistics (JSON) |
| `dashboard/` | Source of the project dashboard pages (HTML) |
| `docs/` | Project log, literature review, evaluation report |

## Reproducing

The dataset is **not included** in this repository. Download `course.json`, `video.json`, `concept.json`, `concept-course.txt`, `concept-video.txt`, `course-field.json` and the undocumented `video_id-ccid.txt` from the MOOCCubeX authors into the project root, then:

1. `python scripts/build_cs_subset.py` creates `subset_cs/`.
2. In WSL2 (Ubuntu 22.04) install Java 11, Hadoop 3.3.6 and Spark 3.5.9, then run `scripts/wsl_setup_hadoop.sh`, `wsl_setup_spark.sh`, `wsl_start_hdfs.sh` and `wsl_ingest_subset.sh`.
3. `scripts/wsl_run_smoke_test.sh` verifies Spark can read the data from HDFS.
4. `pip install jieba stopwordsiso`, then `scripts/wsl_run_module2.sh`.

The shell scripts were written for the author's machine (WSL user `aryan`, project under `C:\Users\ARYAN BHUTANI\Desktop\BDA-Project`); adjust the paths for your setup. Note that WSL stops the Hadoop daemons when no terminal session is open, so use the runner scripts, which start HDFS if needed.

## Team

Aryan Bhutani, Tanmay Sangwan, Tanmay Kandwal.

## Citation

Yu, J., et al. *MOOCCubeX: A Large Knowledge-centered Repository for Adaptive Learning in MOOCs.* CIKM 2021.
