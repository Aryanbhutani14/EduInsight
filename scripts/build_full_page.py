"""Builds dashboard/full.html (the full-corpus results page) from the Module 2-4 stats JSON files.

Styles are reused from dashboard/subset.html so both pages look the same. Cluster names are written by hand
(English) because the corpus terms are Chinese; the script asserts that no CJK text ends up in the page.
Run from the project root:  python scripts/build_full_page.py
"""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DASH = ROOT / "dashboard"
EDA = ROOT / "eda"

m2 = json.loads((EDA / "module2_stats_full.json").read_text(encoding="utf-8"))
m3 = json.loads((EDA / "module3_stats_full.json").read_text(encoding="utf-8"))
m4 = json.loads((EDA / "module4_stats_full.json").read_text(encoding="utf-8"))
pts = json.loads((EDA / "module4_points_full.json").read_text(encoding="utf-8"))

# hand-written English names, from each cluster's top terms and largest courses
LABELS = {
    0: "Music", 1: "Electric circuits", 2: "Thermodynamics and combustion", 3: "Economics",
    4: "Poetry and literature", 5: "Calculus and linear algebra", 6: "C++ programming",
    7: "Programming basics", 8: "Games, film and media", 9: "Education and teaching",
    10: "Anatomy", 11: "Clinical nursing", 12: "Internet and big data", 13: "Geology and soil mechanics",
    14: "Art and painting", 15: "Banking", 16: "Corporate assets and investment",
    17: "Database and literature search", 18: "Academic English writing", 19: "Optics",
    20: "Probability and statistics", 21: "Heating, ventilation, combustion", 22: "Computer networks",
    23: "Materials and manufacturing", 24: "Diagnosis and Chinese medicine", 25: "Immunology",
    26: "Chinese culture and history", 27: "Etiquette and social ethics", 28: "Psychology",
    29: "App and mini-program tutorials", 30: "Engineering drawing", 31: "Law",
    32: "Management and organisations", 33: "Environment and ecology", 34: "Modern Chinese history and politics",
    35: "Cities and development", 36: "Evolution and animals", 37: "Ancient history",
    38: "Cardiology and nursing", 39: "Production and costs", 40: "Software how-to tutorials",
    41: "Wine", 42: "Signals and systems", 43: "Moral and ideological education",
    44: "Sport and skiing", 45: "Cultural relics and archaeology", 46: "Translation and English sentences",
    47: "Construction and project management", 48: "Organic chemistry", 49: "Gardens and plants",
    50: "Philosophy", 51: "Accounting", 52: "Algorithms and geometry", 53: "Entrepreneurship",
    54: "Operating systems and architecture", 55: "Molecular biology", 56: "Nutrition and food",
    57: "Mechanics and physics", 58: "Investment and stocks", 59: "Marketing",
}
CS_CLUSTERS = {6, 7, 12, 17, 22, 29, 40, 52, 54}

clusters = sorted(m4["clusters"], key=lambda c: -c["size"])
assert len(clusters) == 60 and set(LABELS) == {c["cluster"] for c in clusters}

data = {
    "x": pts["x"], "y": pts["y"], "c": pts["c"],
    "labels": [LABELS[i] for i in range(60)],
    "clusters": [{"id": c["cluster"], "size": c["size"], "courses": c["n_courses"],
                  "share": round(c["largest_course_share"] * 100, 1)} for c in clusters],
    "cs": sorted(CS_CLUSTERS),
    "sweep": [{"k": r["k"], "s": r["silhouette"]} for r in m4["k_sweep"]],
    "red": [{"t": t, "pairs": v["pairs"], "videos": v["videos"]} for t, v in m4["redundancy_by_threshold"].items()],
    "jac": [{"lo": b["cosine_range"][0], "j": b["mean_token_jaccard"]} for b in m4["jaccard_by_cosine_bin"]],
    "rand": m4["random_cross_course_mean_jaccard"],
}

sub = (DASH / "subset.html").read_text(encoding="utf-8")
css = sub[sub.index("<style>"):sub.index("</style>") + len("</style>")]
head = sub[:sub.index("<style>")].replace("EduInsight CS Subset", "EduInsight Full Corpus")

def fmt(n):
    return f"{n:,}"

page = head + css + f"""

<div class="topbar">
  <div class="topbar-inner">
    <div class="brand">EduInsight <small>Full Corpus Run</small></div>
    <nav class="site">
      <a href="index.html">Dataset Overview</a>
      <a href="proposal.html">Proposal &amp; Literature</a>
      <a href="pipeline.html">Pipeline &amp; Modules</a>
      <a href="subset.html">CS Subset &amp; EDA</a>
      <a href="full.html" class="active">Full Corpus</a>
    </nav>
  </div>
</div>
<div class="subbar"><div class="subbar-inner">
  <a href="#overview">Overview</a><a href="#scale">Subset vs full</a><a href="#clusters">Clustering</a><a href="#redundancy">Redundancy</a><a href="#queries">Sample queries</a><a href="#limits">Limits</a>
</div></div>

<div class="wrap">

  <section id="overview" class="hero" style="border-top:none;">
    <p class="eyebrow">Scale-up — Modules 2, 3 and 4 on all {fmt(m2['videos'])} videos</p>
    <h1>The full corpus on Spark</h1>
    <p class="sub">The same three Spark jobs that were built and checked on the 4,071-video CS subset were re-run, unchanged in logic, on every captioned video in MOOCCubeX (949 MB of text in HDFS). This page reports what happened at that scale. Everything ran on one laptop in Spark local mode with 8 worker threads, so it shows that the code scales with partitions, not that we have a cluster.</p>
    <div class="stat-grid">
      <div class="stat"><div class="v tab-nums">{fmt(m2['videos'])}</div><div class="l">Videos preprocessed (Module 2)</div></div>
      <div class="stat"><div class="v tab-nums">{m2['tokens_raw']/1e6:.1f} M</div><div class="l">Word tokens before filtering</div></div>
      <div class="stat"><div class="v tab-nums">{fmt(m2['vocab_size'])}</div><div class="l">Distinct terms</div></div>
      <div class="stat"><div class="v tab-nums">{fmt(m3['chunks'])}</div><div class="l">Text chunks embedded (Module 3)</div></div>
      <div class="stat"><div class="v tab-nums">{m4['chosen_k']}</div><div class="l">Topic clusters (Module 4)</div></div>
      <div class="stat"><div class="v tab-nums">{fmt(m4['neighbour_rows'])}</div><div class="l">Nearest-neighbour rows computed</div></div>
    </div>
    <div class="note"><b>Why {fmt(m3['videos_embedded'])} and not {fmt(m2['videos'])} in later stages:</b> {m2['videos'] - m3['videos_embedded']} videos have an empty caption after cleaning, so there is nothing to embed.</div>
  </section>

  <section id="scale">
    <h2>Subset versus full corpus</h2>
    <p class="section-desc">Same code, 14.6 times the videos. Run time grew much more slowly than the data, partly because a Spark job has a fixed start-up cost that dominates the small run.</p>
    <div class="card">
      <div class="tscroll">
        <table class="ptable">
          <thead><tr><th>Stage</th><th>What it does</th><th>CS subset</th><th>Full corpus</th><th>Data grew</th><th>Time grew</th></tr></thead>
          <tbody>
            <tr><td>Module 2</td><td>Tokenise, filter, count terms (RDD + DataFrame)</td><td class="num">29 s</td><td class="num">{m2['runtime_sec']:.0f} s</td><td class="num">15.7× tokens</td><td class="num">4.5×</td></tr>
            <tr><td>Module 3</td><td>Embed chunks on the GPU, average per video</td><td class="num">123 s</td><td class="num">{m3['total_runtime_sec']:.0f} s</td><td class="num">15.8× chunks</td><td class="num">10.7×</td></tr>
            <tr><td>Module 4</td><td>K-means sweep, 10 nearest neighbours, redundancy</td><td class="num">52 s</td><td class="num">{m4['runtime_sec']:.0f} s</td><td class="num">14.6× videos</td><td class="num">6.4×</td></tr>
          </tbody>
        </table>
      </div>
      <div class="note">Embedding throughput went from 489 to {m3['chunks_per_second']:.0f} chunks per second because the GPU stays busier on larger batches. The whole pipeline now takes about {(m2['runtime_sec'] + m3['total_runtime_sec'] + m4['runtime_sec'])/60:.0f} minutes for 59,581 lectures.</div>
    </div>
    <div class="grid-2" style="margin-top:16px;">
      <div class="card">
        <h3>Concept coverage</h3>
        <p class="cap">how many videos carry dataset concept labels</p>
        <div class="stat-grid" style="margin-bottom:0;">
          <div class="stat"><div class="v tab-nums">{fmt(m2['videos_with_clean_concepts'])}</div><div class="l">Videos with a clean concept</div></div>
          <div class="stat"><div class="v tab-nums">{fmt(m2['concepts_clean'])}</div><div class="l">Clean concepts kept of {fmt(m2['concepts_total'])}</div></div>
        </div>
        <div class="note">Only 57% of videos have any dataset concept at all, so any method that relies on the concept labels cannot be the only way to find a lecture.</div>
      </div>
      <div class="card">
        <h3>Does a video's nearest neighbour sit in its own course?</h3>
        <p class="cap">chance that the closest video (by embedding) is from the same course</p>
        <div class="bars" id="chart-nn"></div>
        <div class="note">It drops from 73% to {m3['nearest_neighbour_same_course_pct']}% at full scale. A likely cause is that the full corpus holds many parallel courses on the same subject (several calculus and several Chinese-literature courses), so the closest video is often a near-equivalent lecture from another course. We have not proven that this is the whole explanation.</div>
      </div>
    </div>
  </section>

  <section id="clusters">
    <h2>Topic clusters across all disciplines</h2>
    <p class="section-desc">K-means (cosine, Spark MLlib) was run for k = 10 to 60 and the best silhouette score picked k = 60. t-SNE can only draw a few thousand points, so the map shows a random sample of 8,000 videos; the clustering itself used all {fmt(m4['videos'])}.</p>
    <div class="grid-2">
      <div class="card">
        <h3>Silhouette score by k</h3>
        <p class="cap">higher means tighter, better-separated clusters</p>
        <div id="hist-sweep"></div>
        <div class="note"><b>Honest reading:</b> all scores are low (0.12–0.13), and the best k is at the edge of the range we tried, so a larger k might score slightly higher. Other random seeds agree with this clustering only moderately (adjusted Rand index {min(m4['stability_ari_other_seeds']):.2f}–{max(m4['stability_ari_other_seeds']):.2f}). Treat the clusters as a useful browsing aid, not a ground truth.</div>
      </div>
      <div class="card">
        <h3>Cluster sizes</h3>
        <p class="cap">videos in the 12 largest clusters</p>
        <div class="bars" id="chart-sizes"></div>
        <div class="note">Every cluster mixes videos from many courses (smallest: {min(c['n_courses'] for c in clusters)} courses), and in no cluster does one course make up more than {max(c['largest_course_share'] for c in clusters)*100:.0f}% of the videos. Clusters group by subject, not by course.</div>
      </div>
    </div>

    <div class="card" style="margin-top:16px;">
      <h3>Map of 8,000 sampled lectures</h3>
      <p class="cap">each dot is one video (<b style="color:var(--series-1)">blue</b> = computer-science clusters), placed by t-SNE so that similar lectures sit close together. Labels mark the largest clusters (fewer on narrow screens). Hover a dot for its topic; only local neighbourhoods are meaningful, not the distances between far-apart groups.</p>
      <div id="map-main" class="map-host"><canvas aria-label="t-SNE map of sampled lecture videos, labelled by topic cluster"></canvas></div>
      <div class="note">Computer-science content is spread over {len(CS_CLUSTERS)} clusters (programming, C++, operating systems, networks, algorithms, databases, internet and big data, software tutorials), which matches how the CS subset was chosen but shows that the subset was a thin slice of a much wider corpus.</div>
    </div>

    <div class="card" style="margin-top:16px;">
      <h3>All 60 clusters</h3>
      <p class="cap">names were written by hand from each cluster's most distinctive words and its largest courses</p>
      <div class="tscroll" id="cluster-table"></div>
    </div>
  </section>

  <section id="redundancy">
    <h2>Redundant coverage across courses</h2>
    <p class="section-desc">Each video's 10 nearest neighbours were found with a blockwise cosine join in Spark. Pairs from different courses with very high similarity are candidates for redundant lectures. Counts are lower bounds, because only each video's top 10 neighbours are searched.</p>
    <div class="grid-2">
      <div class="card">
        <h3>Videos with a similar partner in another course</h3>
        <p class="cap">number of videos involved, by cosine similarity threshold</p>
        <div class="bars" id="chart-red"></div>
        <div class="note">At ≥0.90, {fmt(m4['redundancy_by_threshold']['0.9']['videos'])} videos ({m4['redundancy_by_threshold']['0.9']['videos']/m4['videos']*100:.0f}% of the corpus) have a partner in a different course.</div>
      </div>
      <div class="card">
        <h3>Is high similarity real overlap?</h3>
        <p class="cap">mean share of distinct words two lectures have in common, by similarity of the pair</p>
        <div id="hist-overlap"></div>
        <div class="note">Word overlap rises steadily with similarity, from {m4['random_cross_course_mean_jaccard']} for random cross-course pairs to {m4['jaccard_by_cosine_bin'][-1]['mean_token_jaccard']} at ≥0.95, so the embedding similarity does track shared content.</div>
      </div>
    </div>
    <div class="card" style="margin-top:16px;">
      <h3>Which course pairs overlap most</h3>
      <p class="cap">number of video pairs with similarity ≥0.90, for each pair of courses</p>
      <div class="bars wide" id="chart-pairs"></div>
      <div class="note"><b>A finding we did not expect:</b> the biggest overlaps are mostly the <b>same course published twice under different course IDs</b> (for example a law course and its “Tsinghua-certified” version) or videos that belong to no course at all. The top pairs have cosine 1.0 and word overlap 1.0, meaning identical transcripts. In a recommender these must be de-duplicated, otherwise the same lecture would fill several of the top 5 slots.</div>
    </div>
  </section>

  <section id="queries">
    <h2>Sample queries on the full corpus</h2>
    <p class="section-desc">Plain video-level nearest-neighbour search, English query against all 59,512 videos. This is a spot check from Module 3, <b>not</b> the Module 5 engine (translation, chunk matching and keyword fusion were not re-run at full scale). Titles are translated by hand.</p>
    <div class="card">
      <div class="qres">
        <div class="qblock"><div class="qq">process scheduling in an operating system</div>
          <div class="qrow"><span>CPU management and scheduling</span><em>title matches</em><b>0.693</b></div>
          <div class="qrow"><span>2.10 Process management</span><em>title matches</em><b>0.686</b></div>
          <div class="qrow"><span>11.3 Process states</span><em>title matches</em><b>0.670</b></div></div>
        <div class="qblock"><div class="qq">TCP congestion control</div>
          <div class="qrow"><span>TCP congestion control mechanism</span><em>title matches</em><b>0.753</b></div>
          <div class="qrow"><span>4.3 Transmission control protocol</span><em>title matches</em><b>0.735</b></div>
          <div class="qrow"><span>TCP features and segment format</span><em>title matches</em><b>0.684</b></div></div>
        <div class="qblock"><div class="qq">pointers in C</div>
          <div class="qrow"><span>Video (untitled)</span><em>title matches</em><b>0.769</b></div>
          <div class="qrow"><span>8.10 Pointer arrays and multi-level pointers</span><em>title matches</em><b>0.729</b></div>
          <div class="qrow"><span>7.8 Pointer and function (2)</span><em>title matches</em><b>0.723</b></div></div>
        <div class="qblock"><div class="qq">sorting algorithms</div>
          <div class="qrow"><span>5.8 Application of arrays: selection sort</span><em>title matches</em><b>0.659</b></div>
          <div class="qrow"><span>02F-1 Merge sort: the idea</span><em>title matches</em><b>0.646</b></div>
          <div class="qrow"><span>3.3.1 Maintain Tables</span><em>title does not match</em><b>0.701</b></div></div>
        <div class="qblock"><div class="qq">linked list</div>
          <div class="qrow"><span>5.3.1 Syncing global research: SpringerLink database</span><em>about web hyperlinks, not data structures</em><b>0.520</b></div>
          <div class="qrow"><span>6.2.2 Website link exchange (part 2)</span><em>about web hyperlinks, not data structures</em><b>0.493</b></div>
          <div class="qrow"><span>7.4 Creating hyperlinks</span><em>about web hyperlinks, not data structures</em><b>0.490</b></div></div>
        <div class="qblock"><div class="qq">dynamic programming</div>
          <div class="qrow"><span>LinuxCPP1211</span><em>title says nothing about the topic</em><b>0.593</b></div>
          <div class="qrow"><span>7.8 Variable lifetime and local-variable storage</span><em>title does not match</em><b>0.576</b></div>
          <div class="qrow"><span>Video (untitled)</span><em>title does not match</em><b>0.570</b></div></div>
      </div>
      <div class="note">Judged by title only (we did not open the transcripts), four of six queries look right. The two failures are instructive: “linked list” is pulled toward web hyperlinks, probably because the full corpus holds many web-design lectures that the CS subset did not, and “dynamic programming” returns nothing whose title matches. Both are the kind of error the Module 5 translation step and keyword fusion were built to reduce, but we have not measured that at full scale.</div>
    </div>
  </section>

  <section id="limits">
    <h2>What this run does and does not show</h2>
    <div class="card">
      <ul style="margin:0; padding-left:20px; color:var(--ink-2); font-size:13.5px; display:flex; flex-direction:column; gap:8px;">
        <li><b>Shows:</b> the Spark code handles 59,581 videos, {m2['tokens_raw']/1e6:.0f} million tokens and {fmt(m3['chunks'])} embedded chunks end to end, from HDFS input to HDFS output, in about {(m2['runtime_sec'] + m3['total_runtime_sec'] + m4['runtime_sec'])/60:.0f} minutes.</li>
        <li><b>Does not show:</b> multi-node speed-up. This is one machine in local mode, so the evidence is correctness and memory-safe design (blockwise neighbour search, Arrow batches, sampled validation), not cluster scaling.</li>
        <li><b>Not re-evaluated:</b> the Module 5 ranking evaluation (82 queries, hand-judged) was done on the CS subset only. Quality numbers on that page do not transfer automatically to the full corpus.</li>
        <li><b>Not comparable:</b> the “nearest neighbour in the same course” rate and the cluster silhouette differ from the subset because the data and k differ.</li>
        <li><b>Labels are ours:</b> cluster names and translated titles on this page were written by hand and have not been checked by a native Chinese reader.</li>
      </ul>
    </div>
  </section>

  <footer>
    EduInsight — Hadoop-Based Large-Scale Lecture Analytics and Recommendation System · Team: Aryan Bhutani, Tanmay Sangwan, Tanmay Kandwal
  </footer>
</div>
<div id="tooltip"></div>

<script>
(function(){{
  var D = {json.dumps(data, ensure_ascii=False, separators=(',', ':'))};
  var tt = document.getElementById('tooltip');
  function fmt(n){{ return n.toLocaleString('en-US'); }}
""" + r"""
  function renderBars(id, items, opts){
    opts = opts || {};
    var el = document.getElementById(id), max = Math.max.apply(null, items.map(function(d){ return d.value; }));
    items.forEach(function(d){
      var row = document.createElement('div'); row.className = 'bar-row';
      var lab = document.createElement('div'); lab.className = 'lab'; lab.textContent = d.label; lab.title = d.title || d.label;
      var track = document.createElement('div'); track.className = 'bar-track';
      var fill = document.createElement('div'); fill.className = 'bar-fill';
      fill.style.width = Math.max(d.value / max * 100, 2) + '%'; fill.style.background = d.color || 'var(--series-1)';
      track.appendChild(fill);
      track.addEventListener('mousemove', function(e){ tt.style.opacity = 1; tt.style.left = e.clientX + 'px'; tt.style.top = e.clientY + 'px'; tt.textContent = (d.title || d.label) + ': ' + (opts.dec != null ? d.value.toFixed(opts.dec) : fmt(d.value)) + (opts.suffix || ''); });
      track.addEventListener('mouseleave', function(){ tt.style.opacity = 0; });
      var val = document.createElement('div'); val.className = 'val tab-nums'; val.textContent = (opts.dec != null ? d.value.toFixed(opts.dec) : fmt(d.value)) + (opts.suffix || '');
      row.appendChild(lab); row.appendChild(track); row.appendChild(val); el.appendChild(row);
    });
  }
  function niceMax(v){ var mag = Math.pow(10, Math.floor(Math.log(v) / Math.LN10)), n = v / mag; return (n <= 2 ? 2 : n <= 5 ? 5 : 10) * mag; }
  function renderHist(id, bins, opts){
    var el = document.getElementById(id), W = 560, H = 230, padL = 40, padR = 6, padT = 10, padB = 34;
    var plotW = W - padL - padR, plotH = H - padT - padB, n = bins.length, gap = 6, barW = (plotW - gap * (n - 1)) / n;
    var axisMax = opts.max || niceMax(Math.max.apply(null, bins.map(function(b){ return b.value; })));
    var p = ['<svg class="hist-svg" viewBox="0 0 ' + W + ' ' + H + '" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Chart">'];
    for (var g = 0; g <= 4; g++){
      var tv = axisMax * g / 4, y = padT + plotH - plotH * g / 4;
      p.push('<line class="hist-grid" x1="' + padL + '" y1="' + y + '" x2="' + (W - padR) + '" y2="' + y + '"></line>');
      p.push('<text class="hist-ytick" x="' + (padL - 6) + '" y="' + (y + 3) + '" text-anchor="end">' + tv.toFixed(opts.dec) + '</text>');
    }
    bins.forEach(function(b, i){
      var x = padL + i * (barW + gap), bh = b.value / axisMax * plotH, y = padT + plotH - bh;
      p.push('<rect x="' + x.toFixed(1) + '" y="' + y.toFixed(1) + '" width="' + barW.toFixed(1) + '" height="' + Math.max(bh, 1).toFixed(1) + '" rx="3" fill="' + (b.color || 'var(--series-1)') + '"><title>' + b.label + ': ' + b.value.toFixed(opts.dec) + '</title></rect>');
      p.push('<text class="hist-value" x="' + (x + barW / 2).toFixed(1) + '" y="' + (y - 5).toFixed(1) + '" text-anchor="middle">' + b.value.toFixed(opts.dec) + '</text>');
      p.push('<text class="hist-label" x="' + (x + barW / 2).toFixed(1) + '" y="' + (H - padB + 14) + '" text-anchor="middle">' + b.label + '</text>');
    });
    p.push('<line class="hist-axis" x1="' + padL + '" y1="' + (padT + plotH) + '" x2="' + (W - padR) + '" y2="' + (padT + plotH) + '"></line></svg>');
    el.innerHTML = p.join('');
  }

  renderBars('chart-nn', [
    {label:'CS subset', title:'same-course nearest neighbour, CS subset', value:72.9, color:'var(--series-3)'},
    {label:'Full corpus', title:'same-course nearest neighbour, full corpus (6,000-video sample)', value:D_NN, color:'var(--muted)'}
  ], {suffix:'%', dec:1});

  renderHist('hist-sweep', D.sweep.map(function(r){ return {label:'k=' + r.k, value:r.s, color: r.k === 60 ? 'var(--series-1)' : 'var(--muted)'}; }), {dec:3, max:0.16});
  renderHist('hist-overlap', [{label:'random', value:D.rand, color:'var(--muted)'}].concat(D.jac.map(function(b){ return {label: b.lo === 0 ? '<0.80' : b.lo === 0.95 ? '0.95+' : String(b.lo.toFixed(2)), value:b.j}; })), {dec:2});

  renderBars('chart-sizes', D.clusters.slice(0, 12).map(function(c){ return {label:D.labels[c.id], title:D.labels[c.id] + ' (' + c.courses + ' courses)', value:c.size}; }));
  renderBars('chart-red', D.red.map(function(r){ return {label:'similarity ≥ ' + r.t, title:fmt(r.pairs) + ' cross-course pairs; videos involved', value:r.videos}; }));
  renderBars('chart-pairs', [
    {label:'No course ↔ no course', title:'videos that belong to no course, similar to each other', value:831},
    {label:'Moral cultivation (2 runs)', title:'Moral Cultivation and Legal Basics, two course IDs with the same name', value:547},
    {label:'Modern Chinese history (2 runs)', title:'Outline of Modern Chinese History, two course IDs with the same name', value:518},
    {label:'Calculus ↔ Calculus (limits)', title:'Calculus ↔ Calculus: limits and single-variable functions', value:475},
    {label:'Ci poetry ↔ Chinese lit. (Song+)', title:'Tang and Song Ci Poetry ↔ University Chinese: Northern Song to modern', value:454},
    {label:'Ci poetry ↔ Chinese lit. (to Tang)', title:'Tang and Song Ci Poetry ↔ University Chinese: Pre-Qin to Tang', value:331},
    {label:'Organic chem ↔ Concise organic', title:'Organic Chemistry ↔ Concise Organic Chemistry', value:329},
    {label:'Physics 2 ↔ Physics prep (E&M)', title:'University Physics 2 ↔ University Physics (prep): electromagnetism', value:328}
  ]);

  // ---------- cluster map ----------
  function cssVar(n){ return getComputedStyle(document.documentElement).getPropertyValue(n).trim(); }
  function median(a){ var s = a.slice().sort(function(p, q){ return p - q; }); return s[Math.floor(s.length / 2)]; }
  var cent = {}, top20 = {};
  function prep(cv, w, h){ var dpr = window.devicePixelRatio || 1; cv.width = Math.round(w * dpr); cv.height = Math.round(h * dpr); cv.style.width = w + 'px'; cv.style.height = h + 'px'; var ctx = cv.getContext('2d'); ctx.setTransform(dpr, 0, 0, dpr, 0, 0); return ctx; }
  function drawMap(cv, w, h){
    var ctx = prep(cv, w, h), n = D.x.length, i, pad = 18, r = 1.9;
    var csFlag = {}; D.cs.forEach(function(k){ csFlag[k] = 1; });
    function px(j){ return pad + D.x[j] / 1000 * (w - 2 * pad); }
    function py(j){ return pad + D.y[j] / 1000 * (h - 2 * pad); }
    ctx.clearRect(0, 0, w, h);
    ctx.fillStyle = cssVar('--muted'); ctx.globalAlpha = 0.4; ctx.beginPath();
    for (i = 0; i < n; i++){ if (!csFlag[D.c[i]]){ ctx.moveTo(px(i) + r, py(i)); ctx.arc(px(i), py(i), r, 0, 6.2832); } }
    ctx.fill();
    ctx.fillStyle = cssVar('--series-1'); ctx.globalAlpha = 0.85; ctx.beginPath();
    for (i = 0; i < n; i++){ if (csFlag[D.c[i]]){ ctx.moveTo(px(i) + r, py(i)); ctx.arc(px(i), py(i), r, 0, 6.2832); } }
    ctx.fill(); ctx.globalAlpha = 1;
    ctx.font = '600 ' + (w < 520 ? 10 : 11.5) + 'px "IBM Plex Sans", sans-serif'; ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
    var maxLab = w < 520 ? 7 : 20;
    Object.keys(top20).forEach(function(k){
      if (top20[k] > maxLab) return;
      var x = pad + cent[k][0] / 1000 * (w - 2 * pad), y = pad + cent[k][1] / 1000 * (h - 2 * pad);
      var tw = ctx.measureText(D.labels[k]).width / 2 + 4; x = Math.max(tw, Math.min(w - tw, x));
      ctx.lineWidth = 4; ctx.strokeStyle = cssVar('--surface'); ctx.strokeText(D.labels[k], x, y);
      ctx.fillStyle = cssVar('--ink'); ctx.fillText(D.labels[k], x, y);
    });
  }
  function redraw(){
    var host = document.getElementById('map-main'), cv = host.querySelector('canvas'), w = host.clientWidth;
    drawMap(cv, w, w < 520 ? 400 : Math.max(320, Math.min(560, Math.round(w * 0.62))));
  }
  (function initMap(){
    var xs = {}, ys = {}, i, k, n = D.x.length;
    for (i = 0; i < n; i++){ k = D.c[i]; (xs[k] = xs[k] || []).push(D.x[i]); (ys[k] = ys[k] || []).push(D.y[i]); }
    Object.keys(xs).forEach(function(key){ cent[key] = [median(xs[key]), median(ys[key])]; });
    D.clusters.slice(0, 20).forEach(function(c, r){ top20[c.id] = r + 1; });
    var cvm = document.querySelector('#map-main canvas');
    cvm.addEventListener('mousemove', function(e){
      var rect = cvm.getBoundingClientRect(), w = rect.width, h = rect.height, pad = 18, mx = e.clientX - rect.left, my = e.clientY - rect.top, best = -1, bd = 100;
      for (var j = 0; j < n; j++){ var dx = pad + D.x[j] / 1000 * (w - 2 * pad) - mx, dy = pad + D.y[j] / 1000 * (h - 2 * pad) - my, d = dx * dx + dy * dy; if (d < bd){ bd = d; best = j; } }
      if (best >= 0){ tt.style.opacity = 1; tt.style.left = e.clientX + 'px'; tt.style.top = e.clientY + 'px'; tt.textContent = D.labels[D.c[best]]; } else { tt.style.opacity = 0; }
    });
    cvm.addEventListener('mouseleave', function(){ tt.style.opacity = 0; });
    redraw();
    var timer; window.addEventListener('resize', function(){ clearTimeout(timer); timer = setTimeout(redraw, 150); });
    new MutationObserver(redraw).observe(document.documentElement, {attributes:true, attributeFilter:['data-theme']});
    if (window.matchMedia){ var mq = window.matchMedia('(prefers-color-scheme: dark)'); if (mq.addEventListener) mq.addEventListener('change', redraw); }
    if (document.fonts && document.fonts.ready) document.fonts.ready.then(redraw);
  })();

  var csFlag2 = {}; D.cs.forEach(function(k){ csFlag2[k] = 1; });
  document.getElementById('cluster-table').innerHTML = '<table class="ptable"><thead><tr><th>Topic cluster</th><th>Videos</th><th>Courses</th><th>Largest course share</th></tr></thead><tbody>' +
    D.clusters.map(function(c){ return '<tr><td>' + D.labels[c.id] + (csFlag2[c.id] ? ' <em>(CS-related)</em>' : '') + '</td><td class="num">' + fmt(c.size) + '</td><td class="num">' + c.courses + '</td><td class="num">' + c.share + '%</td></tr>'; }).join('') + '</tbody></table>';
})();
</script>
"""
page = page.replace("D_NN", str(m3["nearest_neighbour_same_course_pct"]))

assert not re.search(r"[　-鿿＀-￯]", page), "CJK text found in generated page"
(DASH / "full.html").write_text(page, encoding="utf-8")
print("wrote", DASH / "full.html", len(page), "bytes")
