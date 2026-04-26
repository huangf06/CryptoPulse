import fitz, re, json

doc = fitz.open(r'mock_exam/Certified Data Engineer Professional  327题.pdf')
full_text = ''
for page in doc:
    full_text += page.get_text()

# Split into questions
questions_raw = re.split(r'(?=QUESTION \d+\n)', full_text)
q_dict = {}
for q in questions_raw:
    m = re.match(r'QUESTION (\d+)\n', q)
    if m:
        qnum = int(m.group(1))
        clean = re.sub(r'tps://shop.*?\n|https://shop.*?\n', '', q.strip())
        clean = re.sub(r'店铺.*?\n', '', clean)
        # Also extract question-only text (before Correct Answer) for classification
        ans_pos = re.search(r'Correct Answer:', clean)
        q_only = clean[:ans_pos.start()] if ans_pos else clean[:500]
        q_dict[qnum] = {'full': clean, 'question_only': q_only}

# Extract all 121 wrong questions from review_notes
with open('mock_exam/review_notes.md', encoding='utf-8') as f:
    notes = f.read()

wrong_pattern = re.compile(r'### Q(\d+) ❌ — (.+?)(?:\n|$)')
answer_pattern = re.compile(r'\*\*我的答案[：:]\*\*\s*(\S+)\s*\|\s*\*\*正确答案[：:]\*\*\s*(\S+)')

parts = re.split(r'(?=### Q\d+ ❌)', notes)
wrong_questions = []
for part in parts:
    m = wrong_pattern.match(part)
    if m:
        qnum = int(m.group(1))
        title = m.group(2).strip()
        ans_m = answer_pattern.search(part)
        my_ans = ans_m.group(1) if ans_m else '?'
        correct = ans_m.group(2) if ans_m else '?'
        is_pending = '待补充' in title
        wrong_questions.append({
            'num': qnum,
            'title': title,
            'my_answer': my_ans,
            'correct': correct,
            'is_pending': is_pending,
            'full_notes': part.strip() if not is_pending else None,
            'pdf_full': q_dict.get(qnum, {}).get('full', ''),
            'pdf_question': q_dict.get(qnum, {}).get('question_only', '')
        })

# Classify with priority ordering
topic_definitions = [
    ('delta_sharing', 'Delta Sharing', [
        'Delta Sharing', 'delta sharing', 'share model',
        'Databricks-to-Databricks sharing', 'open sharing', 'recipient.*share'
    ]),
    ('cdf_cdc', 'Change Data Feed / CDC', [
        'Change Data Feed', 'CDF', 'CDC', '_change_type', 'readChangeFeed',
        'table_changes', 'change data capture', 'SCD Type'
    ]),
    ('auto_loader', 'Auto Loader', [
        'Auto Loader', 'cloudFiles', 'autoLoader', 'directory listing mode',
        'notification mode', 'rescue data'
    ]),
    ('dlt_lakeflow', 'DLT / Lakeflow Pipelines', [
        r'\bDLT\b', 'Lakeflow', 'LIVE TABLE', 'STREAMING LIVE TABLE', 'Expectations',
        'APPLY CHANGES', 'Declarative Pipeline', 'event log'
    ]),
    ('structured_streaming', 'Structured Streaming', [
        'readStream', 'writeStream', 'checkpoint', 'Structured Streaming',
        'streaming query', r'\btrigger\b', 'microbatch', 'watermark', 'foreachBatch',
        'streaming pipeline', 'streaming data pipeline', 'availableNow',
        'processingTime', 'near real-time', 'near-real-time', 'streaming.*ingest'
    ]),
    ('unity_catalog', 'Unity Catalog 与权限管理', [
        'Unity Catalog', 'USE CATALOG', 'USE SCHEMA', r'\bGRANT\b', r'\bREVOKE\b',
        'ALL PRIVILEGES', r'\bMANAGE\b', 'OWNERSHIP', 'Column Mask', 'Row Filter',
        'External Location', 'Storage Credential', 'permission management',
        'privilege', 'workspace admin.*catalog', 'delegate permission', 'access control',
        r'catalog.*permission', r'permission.*catalog'
    ]),
    ('jobs_workflows', 'Jobs & Workflows', [
        'Jobs API', 'jobs/create', 'run-now', 'runs/submit', 'runs/repair',
        'Multi-task', 'Job run', r'\bworkflow\b', 'orchestrat', 'job parameter',
        r'dbutils\.widgets', 'reporting Job', 'If/else condition', 'task dependenc',
        'jobs/runs', 'Jobs REST'
    ]),
    ('performance', '性能优化与 Spark 调优', [
        'Spark UI', r'\bperformance\b', r'\bspill\b', r'\bshuffle\b', r'\bbroadcast\b',
        'predicate push', 'data skew', r'\brepartition\b', 'memory-optimized',
        r'\bPhoton\b', 'query plan', r'\bdisk spill\b', 'MERGE.*optim'
    ]),
    ('delta_lake', 'Delta Lake 核心机制', [
        'Delta Lake', 'delta lake', 'Transaction Log', 'Data Skipping',
        r'\bVACUUM\b', 'Time Travel', 'Schema Evolution', 'Auto Compaction',
        r'\bOPTIMIZE\b', 'Liquid Clustering', 'Z-ORDER', 'Deletion Vector',
        'MERGE INTO', 'CHECK Constraint', 'Managed Table', 'External Table',
        'Predictive Optimization', r'\bCLONE\b', 'liquid cluster', 'deletion vector',
        r'\bvacuum\b', 'mergeSchema', 'Delta table', 'delta table', 'Delta format',
        r'\bUniform\b', 'Iceberg'
    ]),
    ('cli_api', 'Databricks CLI, API 与部署', [
        r'\bCLI\b', 'databricks fs', 'databricks clusters', 'REST API',
        'Asset Bundle', 'Terraform', 'Service Principal', 'infrastructure.*provision',
        r'databricks.*create', 'databricks pipelines'
    ]),
    ('git_repos', 'Git, Repos 与测试', [
        r'\bRepos\b', r'\bGit\b', 'branch', 'version control', 'Files in Repos',
        'unit test', r'modular.*testable', 'DataFrame.transform'
    ]),
    ('platform_ops', '平台操作与工具', [
        r'dbutils\.secrets', r'\bnotebook\b', r'%pip', r'%run', r'%sh',
        r'sys\.path', r'\bcluster\b', 'init script', r'\bpool\b', 'Pandas UDF',
        'applyInPandas', r'\bMLflow\b', 'SQL Alert', r'\bDashboard\b',
        r'\bwarehouse\b', r'\bPII\b', r'\bmasking\b', r'\bSHA\b', r'\bhash\b',
        'Python library', 'access token', 'image files', 'JPEG', 'ingest.*files',
        r'\bsecrets\b'
    ]),
]

classified = {t[0]: [] for t in topic_definitions}
classified['other'] = []

for q in wrong_questions:
    # Use ONLY question text (before Correct Answer) + title for classification
    # This avoids false positives from explanation text
    search_text = q['pdf_question'] + ' ' + q['title']
    matched = False
    for topic_key, topic_name, keywords in topic_definitions:
        for kw in keywords:
            if re.search(kw, search_text, re.IGNORECASE):
                classified[topic_key].append(q)
                matched = True
                break
        if matched:
            break
    if not matched:
        classified['other'].append(q)

# Print summary
print("=== Final Classification ===\n")
total = 0
for topic_key, topic_name, _ in topic_definitions:
    qs = classified[topic_key]
    if qs:
        nums = ', '.join([f"Q{q['num']}" for q in qs])
        pending_count = sum(1 for q in qs if q['is_pending'])
        pending_str = f" ({pending_count} pending)" if pending_count else ""
        print(f"{topic_name}: {len(qs)}{pending_str}")
        print(f"  {nums}")
        total += len(qs)

if classified['other']:
    print(f"\nOther: {len(classified['other'])}")
    for q in classified['other']:
        print(f"  Q{q['num']}: {q['title']}")
    total += len(classified['other'])

print(f"\nTotal: {total}")

# Save full classification
output = {}
for topic_key, topic_name, _ in topic_definitions:
    qs = classified[topic_key]
    if qs:
        output[topic_key] = {
            'name': topic_name,
            'count': len(qs),
            'questions': [{
                'num': q['num'],
                'title': q['title'],
                'my_answer': q['my_answer'],
                'correct': q['correct'],
                'is_pending': q['is_pending'],
                'pdf_text': q['pdf_full']
            } for q in qs]
        }

if classified['other']:
    output['other'] = {
        'name': 'Other',
        'count': len(classified['other']),
        'questions': [{
            'num': q['num'],
            'title': q['title'],
            'my_answer': q['my_answer'],
            'correct': q['correct'],
            'is_pending': q['is_pending'],
            'pdf_text': q['pdf_full']
        } for q in classified['other']]
    }

json.dump(output, open('mock_exam/_final_classification.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
print("\nSaved to mock_exam/_final_classification.json")
