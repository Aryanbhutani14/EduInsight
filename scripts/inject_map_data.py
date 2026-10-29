import json
import os
import re

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CJK = re.compile(r"[一-鿿]")

# English names for the 59 CS-subset courses and for the cluster terms, so the dashboard shows no Chinese
COURSE_EN = {
    "5G与人工智能": "5G and Artificial Intelligence",
    "ARM微控制器与嵌入式系统": "ARM Microcontrollers & Embedded Systems",
    "C++语言程序设计基础": "C++ Programming Fundamentals",
    "C++语言程序设计进阶": "C++ Programming (Advanced)",
    "Combinatorial Mathematics（2020）": "Combinatorial Mathematics (2020)",
    "C君带你玩编程": "C Programming with C-Jun",
    "C语言程序设计（上）": "C Programming (Part 1)",
    "C语言程序设计（下）": "C Programming (Part 2)",
    "JAVA程序设计进阶": "Java Programming (Advanced)",
    "Java程序设计": "Java Programming",
    "Linux 内核分析与应用": "Linux Kernel Analysis & Applications",
    "Linux导论": "Introduction to Linux",
    "Linux系统管理": "Linux System Administration",
    "Petri网：模型、理论与应用": "Petri Nets: Models, Theory & Applications",
    "VC++面向对象与可视化程序设计（上）：Windows编程基础": "VC++ OOP & Visual Programming (Part 1): Windows Basics",
    "VC++面向对象与可视化程序设计（下）：MFC编程基础 (自主模式)": "VC++ OOP & Visual Programming (Part 2): MFC Basics",
    "“做中学”Java程序设计": "Java Programming (Learning by Doing)",
    "万物互联": "Internet of Everything",
    "互联网大规模数据分析技术": "Large-Scale Internet Data Analysis",
    "信息技术与教育应用": "Information Technology in Education",
    "信息素养-互联网+时代的学习与生活": "Information Literacy in the Internet Era",
    "单片机原理及应用": "Microcontroller Principles & Applications",
    "基于Linux的C++": "C++ on Linux",
    "基于R语言的社会统计分析": "Social Statistics with R",
    "大学计算机基础与应用": "University Computer Fundamentals & Applications",
    "大数据与城市规划": "Big Data & Urban Planning",
    "大数据平台核心技术": "Core Technologies of Big Data Platforms",
    "大数据机器学习": "Big Data Machine Learning",
    "学做小程序——基础篇": "Learn to Build Mini-Programs: Basics",
    "嵌入式系统设计": "Embedded Systems Design",
    "微机原理与接口技术": "Microcomputer Principles & Interfacing",
    "微软亚洲研究院大数据系列讲座（2018春）": "Microsoft Research Asia Big Data Lectures (2018)",
    "操作系统": "Operating Systems",
    "数据库系统原理与开发": "Database Systems: Principles & Development",
    "数据挖掘：理论与算法": "Data Mining: Theory & Algorithms",
    "数据科学导论": "Introduction to Data Science",
    "数据结构(上)": "Data Structures (Part 1)",
    "数据结构（Data Structures）": "Data Structures",
    "智能设备应用开发（基于Android平台）": "Smart Device App Development (Android)",
    "极速悟透3ds Max": "3ds Max Crash Course",
    "汇编语言程序设计": "Assembly Language Programming",
    "电子信息科学与技术导引(1)": "Introduction to Electronic Information Science & Technology (1)",
    "算法设计与分析": "Algorithm Design & Analysis",
    "网络、群体与市场": "Networks, Crowds & Markets",
    "网络安全概述（自主模式）": "Network Security Overview",
    "网络技术与应用": "Network Technology & Applications",
    "网络综合布线": "Structured Cabling for Networks",
    "计算思维和数据科学导论": "Computational Thinking & Data Science",
    "计算思维导论": "Introduction to Computational Thinking",
    "计算机操作系统": "Computer Operating Systems",
    "计算机科学和Python编程导论（课程迁移）": "Intro to Computer Science & Python (migrated course)",
    "计算机网络": "Computer Networks",
    "计算机辅助设计": "Computer-Aided Design",
    "软件工程": "Software Engineering",
    "软件理论基础": "Foundations of Software Theory",
    "面向对象程序设计（C++）": "Object-Oriented Programming (C++)",
    "高级大数据系统": "Advanced Big Data Systems",
    "高级数据库系统": "Advanced Database Systems",
    "高级语言程序设计": "High-Level Language Programming",
}
TERM_EN = {
    "寄存器": "register", "指令": "instruction", "地址": "address", "里头": "inside (filler word)",
    "整数": "integer", "等于": "equals", "运算": "operation", "变量": "variable", "表达式": "expression",
    "算法": "algorithm", "元素": "element", "此前": "previously (filler word)", "位置": "position",
    "依然": "still (filler word)", "程序设计": "programming", "语言": "language", "对象": "object", "成员": "member",
    "知识": "knowledge", "发展": "development", "人类": "humans", "研究": "research", "世界": "world",
    "传输": "transmission", "网络": "network", "协议": "protocol", "路由器": "router", "通信": "communication",
    "概率": "probability", "样本": "sample", "假设": "hypothesis", "估计": "estimate",
    "数据库": "database", "分布式": "distributed", "服务器": "server", "查询": "query", "用户": "user",
    "显示": "display", "按钮": "button", "添加": "add", "点击": "click", "文件": "file",
    "目录": "directory", "操作系统": "operating system", "文件系统": "file system", "内核": "kernel",
    "节点": "node", "一棵": "\"a\" (counting word for trees)", "树根": "tree root", "孩子": "child", "树中": "in the tree",
    "城市": "city", "北京": "Beijing", "交通": "traffic", "道路": "road", "城市规划": "urban planning", "中断": "interrupt",
}

points = json.load(open(os.path.join(BASE, "eda", "module4_points.json"), encoding="utf-8"))
points["courses"] = [COURSE_EN.get(c, c) for c in points["courses"]]
for d in points["clusters"]:
    d["terms"] = [TERM_EN.get(t, t) for t in d["terms"]]
payload = json.dumps(points, ensure_ascii=False, separators=(",", ":"))
left = sorted(set(CJK.findall(payload)))
assert not left, "untranslated Chinese left in the map data: " + "".join(left)

path = os.path.join(BASE, "dashboard", "subset.html")
html = open(path, encoding="utf-8").read()
new, n = re.subn(r"/\*MAPDATA\*/.*?/\*ENDMAPDATA\*/",
                 lambda m: "/*MAPDATA*/" + payload + "/*ENDMAPDATA*/", html, count=1, flags=re.S)
assert n == 1, "marker not found in subset.html"
open(path, "w", encoding="utf-8").write(new)
print(f"injected {len(points['x'])} points ({len(payload)//1024} KB, English only) into dashboard/subset.html")
