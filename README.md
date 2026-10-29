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
| Module 3: Multilingual embeddings | Done |
| Module 4: Similarity and clustering | Done |
| Module 5: Ranking and recommendation engine | Planned |
| Module 6: Integration and query demo | Planned |

## Results so far

- **Data verification:** the first `video.json` download was truncated (14 MB vs about 580 MB); re-downloaded and validated line by line (59,581 valid records). The official README omits the file that links course resources to video captions (`relations/video_id-ccid.txt`); it was found through the dataset's GitHub issue tracker and verified.
- **CS development subset:** 59 Computer Science courses with at least 10 captioned videos each, 4,071 videos, 590.9 hours, 27,552 concepts.
- **EDA finding:** 2.9% of concepts are 1–2 character generic words that dominate raw frequency rankings; they are filtered in Module 2.
- **Module 2 (Spark, 29 s on the subset):** 5.94M raw tokens reduced to 1.80M, 61,094-term vocabulary, 26,766 clean concepts, 3,938 of 4,071 videos linked to clean concepts. Outputs are Parquet files in HDFS under `/eduinsight/processed/`.
- **Module 3 (Spark + GPU, 123 s):** captions are split into chunks that fit the model's 128-token limit and encoded with `paraphrase-multilingual-mpnet-base-v2`; chunk vectors are averaged into one 768-d vector per video (4,071 videos, 48,944 chunks). Videos in the same course are closer than videos in different courses (mean cosine 0.638 vs 0.483), and English queries retrieve relevant Chinese lectures for topics such as TCP congestion control and process scheduling. Known weak queries: "dynamic programming" and "linked list". Statistics are in `eda/module3_stats.json`.
- **Module 4 (Spark MLlib, 52 s):** K-means on the video vectors (cosine distance, k=12 chosen from a sweep by silhouette score) gives 12 topic clusters, each mixing at least 9 courses. A distributed Spark job finds the 10 nearest neighbours of every video. 447 cross-course video pairs have cosine similarity of at least 0.90 (458 videos); reading them shows the same topic taught by different lecturers, not copied lectures. Limits: silhouette is low (about 0.11) and other random seeds agree only moderately (ARI 0.57–0.65). Statistics are in `eda/module4_stats.json`.

HDFS runs single-node (pseudo-distributed, replication 1) and Spark runs in `local[*]` mode on one machine.

## Repository layout

| Path | Contents |
|---|---|
| `scripts/build_cs_subset.py` | Builds the CS subset from the full dataset files |
| `scripts/eda_cs_subset.py`, `scripts/dashboard_stats.py` | EDA charts and corpus statistics |
| `scripts/wsl_*.sh`, `scripts/start_eduinsight_cluster.sh`, `scripts/stop_eduinsight_cluster.sh` | Hadoop/Spark setup, HDFS ingestion, cluster start/stop (WSL2 Ubuntu 22.04) |
| `scripts/spark_smoke_test.py` | Module 1 check: Spark reading from HDFS |
| `scripts/spark_module2_preprocess.py` | Module 2 Spark job |
| `scripts/spark_module3_embed.py`, `scripts/m3_model_check.py`, `scripts/wsl_install_m3_deps.sh`, `scripts/wsl_run_module3.sh` | Module 3: embedding job, model check, dependency install, runner |
| `scripts/spark_module4_cluster.py`, `scripts/wsl_run_module4.sh`, `scripts/inject_map_data.py` | Module 4: clustering and similarity job, runner, and the script that puts the cluster map data into the dashboard page |
| `eda/` | Generated charts and statistics (JSON) |
| `dashboard/` | Source of the project dashboard pages (HTML) |
| `docs/` | Project log, literature review, evaluation report |

## Reproducing

The dataset is **not included** in this repository. Download `course.json`, `video.json`, `concept.json`, `concept-course.txt`, `concept-video.txt`, `course-field.json` and the undocumented `video_id-ccid.txt` from the MOOCCubeX authors into the project root, then:

1. `python scripts/build_cs_subset.py` creates `subset_cs/`.
2. In WSL2 (Ubuntu 22.04) install Java 11, Hadoop 3.3.6 and Spark 3.5.9, then run `scripts/wsl_setup_hadoop.sh`, `wsl_setup_spark.sh`, `wsl_start_hdfs.sh` and `wsl_ingest_subset.sh`.
3. `scripts/wsl_run_smoke_test.sh` verifies Spark can read the data from HDFS.
4. `pip install jieba stopwordsiso`, then `scripts/wsl_run_module2.sh`.
5. Module 3 needs an NVIDIA GPU visible inside WSL: run `scripts/wsl_install_m3_deps.sh` (PyTorch, sentence-transformers; several GB), then `scripts/wsl_run_module3.sh check` and `scripts/wsl_run_module3.sh embed`.
6. `scripts/wsl_run_module4.sh` runs Module 4 (needs the Module 3 output in HDFS). `python scripts/inject_map_data.py` then refreshes the cluster map in `dashboard/subset.html`.

The shell scripts were written for the author's machine (WSL user `aryan`, project under `C:\Users\ARYAN BHUTANI\Desktop\BDA-Project`); adjust the paths for your setup. Note that WSL stops the Hadoop daemons when no terminal session is open, so use the runner scripts, which start HDFS if needed.

## Team

Aryan Bhutani, Tanmay Sangwan, Tanmay Kandwal.

## Citation

Yu, J., et al. *MOOCCubeX: A Large Knowledge-centered Repository for Adaptive Learning in MOOCs.* CIKM 2021.
