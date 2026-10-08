# -*- coding: utf-8 -*-
"""
Information-Gatherer v2 - 通用个人信息研究/职业情报系统
================================================
核心设计：
    搜索 -> L0_RAW -> L1_INTEL -> L2_FACT -> L3_KNOWLEDGE -> COMMAND

本版本重点：
1. 保留旧版目录、文件和参数，尽量无缝升级。
2. LLM 默认单线程；搜索/网页抓取可以多线程，并带请求频率限制。
3. 每个“搜索/总结/晋升”步骤都写入 RESULT/logs/pipeline.log。
4. 采用“状态文件头 + 事件日志”双重断点机制：
   - 正常结束：日志记录 DONE。
   - 中途关闭：没有 DONE 的任务会在下一次启动时继续。
5. L1->L2 改为“小批次晋升”，避免一次塞入 5 万字符导致本地模型长时间无响应。
6. LLM 超时/异常不会让整个程序崩溃；会记录 ERROR，下一次运行可继续。
7. 当前时间会注入搜索查询和所有关键 LLM Prompt，区分“当前信息”和“长期趋势”。
8. 提前预留 local/online 多模型配置；本版本只启用一个 provider。
9. 不使用数据库；所有信息均为 TXT/LOG/JSON。
"""

import argparse
import concurrent.futures
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

try:
    from colorama import init as colorama_init
except Exception:
    colorama_init = None


# ============================================================
# 1. 基础路径
# ============================================================

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "set.json"
RESULT = ROOT / "RESULT"

L0 = RESULT / "L0_RAW"
L1 = RESULT / "L1_INTEL"
L2 = RESULT / "L2_FACT"
L3 = RESULT / "L3_KNOWLEDGE"
COMMAND = RESULT / "COMMAND"
ARCHIVE = RESULT / "ARCHIVE"
REPORTS = RESULT / "reports"
LOGS = RESULT / "logs"

PIPELINE_LOG = LOGS / "pipeline.log"
ERROR_LOG = LOGS / "errors.log"


# ============================================================
# 2. 通用提示词
# 个人信息、研究主题、搜索模板等可传播信息全部从 set.json/profile.txt 读取。
# 这样主程序源码即使被传播，也不会直接携带用户个人信息。
# ============================================================

# ============================================================
# 3. Prompt
#    重要：当前时间由 current_context() 动态注入，避免把 2026 年
#    的招聘信息误当成四年后的确定事实。
# ============================================================

BASE_SYSTEM = """你是一个通用信息研究 Agent。
你的工作对象可以是职业、学校、科研、科技、产品、历史、商业、政策、人物、项目、游戏、技术或用户指定的任何主题。

核心规则：
1. 不编造事实、数字、政策、招聘、人物经历、价格或结论。
2. 明确区分：事实、来源、趋势、推断、建议、未知。
3. 优先使用官方/政府/高校/科研机构/原始数据；社区经验只能作为补充证据。
4. 每个重要事实尽量保留来源、URL、发布时间或数据年份。
5. 当前时间和历史时间必须分开；过去事实不能自动变成未来确定事实。
6. 主动寻找反例、冲突来源和可能导致结论失效的条件。
7. 证据不足时必须写“未知/待核验”，禁止为了完整而猜测。
8. 不要强迫把研究结果转化成行动；只有研究配置或证据支持时才提出行动。
9. 用户画像只用于相关任务；不要把与当前研究无关的私人信息传播到搜索查询中。
10. 中文输出，结构清晰，可追溯。
"""

PROMPTS = {
    "extract": BASE_SYSTEM + """
任务：从原始搜索结果/网页中提取可核验事实。
输出：
【标题】
【来源】
【URL】
【发布时间/数据年份】
【本次核验时间】
【主题】
【关键事实】
【数字/条件/范围】
【证据等级】A官方/原始数据 B多源 C单源 D经验 E推测
【仍需核验】
""",
    "fact": BASE_SYSTEM + """
任务：对多份情报进行事实核验、去重、冲突处理和趋势整理。
官方来源优先；新年份优先用于当前状态，旧年份用于历史趋势；不同年份不得混写。
输出：【已确认事实】【来源】【数据年份】【证据等级】【冲突/不确定】【趋势判断】【仍需补证】
""",
    "knowledge": BASE_SYSTEM + """
任务：把已经核验的事实提炼成可复用知识。
必须回答：主题是什么、已知事实、变化趋势、关键变量、影响、机会、风险、反例、证据缺口。
不要凭空加入用户行动建议。
输出：【当前知识结论】【趋势】【机会】【风险】【反例】【证据缺口】
""",
    "devil": BASE_SYSTEM + """
你是反方审计 Agent。攻击给定结论：证据是否不足？是否单一来源？是否以少数案例代表总体？是否混淆时间？是否忽略反例？
输出：【结论漏洞】【反例】【需要补证】【建议保留/降低/否决】
""",
    "decision": BASE_SYSTEM + """
你是研究报告总编辑。
输入：研究目标、可用画像（如有）、已核验知识和反方审计。
输出一份“当前研究结论”，而不是强行制定人生规划。
包含：【核心结论】【关键证据】【趋势】【不确定性】【不同方案/解释比较】【建议关注的下一步研究】【明确禁止过度推断的地方】。
只有配置明确要求时才生成行动计划。
"""
}


# ============================================================
# 4. 通用工具
# ============================================================

# 函数：now
def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# 函数：current_context
def current_context():
    """动态生成时间上下文，防止长期规划把当前招聘信息永久化。"""
    dt = datetime.now()
    return (
        f"当前本机时间：{dt:%Y-%m-%d %H:%M:%S}\n"
        f"当前年份：{dt.year}\n"
        f"当前月份：{dt.month}\n"
        "重要：当前招聘/政策/考试信息属于当前时点信息；"
        "未来规划必须区分历史规律、当前事实和未来推测。"
    )


# 函数：stage_policy_context
def stage_policy_context(cfg):
    # 将用户在set.json定义的阶段规则注入关键模型请求；程序本身不预设任何职业阶段。
    research = cfg.get("research", {})
    policy = research.get("stage_policy", {})
    stage = cfg.get("research", {}).get("current_stage", "default")
    text = policy.get(stage, "")
    return f"当前研究阶段：{stage}\n阶段行为约束：{text}"


# 函数：safe_name
def safe_name(s, max_len=90):
    s = re.sub(r'[\\/:*?"<>|]+', "_", s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return (s[:max_len] or "未命名")


# 函数：sha
def sha(s):
    return hashlib.sha256(str(s).encode("utf-8", "ignore")).hexdigest()[:16]


# 函数：write_text
def write_text(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


# 函数：read_text
def read_text(path):
    return path.read_text(encoding="utf-8", errors="ignore")


# 函数：setup_dirs
def setup_dirs(cfg=None):
    for p in [L0, L1, L2, L3, COMMAND, ARCHIVE, REPORTS, LOGS]:
        p.mkdir(parents=True, exist_ok=True)

    categories = (cfg or {}).get("research", {}).get("topics", [])
    for cat in categories:
        for layer in [L1, L2, L3]:
            (layer / safe_name(cat)).mkdir(parents=True, exist_ok=True)

    (COMMAND / "历史").mkdir(parents=True, exist_ok=True)
    for name in ["daily", "weekly", "monthly", "deep_research"]:
        (REPORTS / name).mkdir(parents=True, exist_ok=True)


# ============================================================
# 5. 日志与断点
# ============================================================

_log_lock = threading.Lock()


# 函数：event_log
def event_log(event, stage="", task="", source="", output="", detail=""):
    """
    每个关键步骤写一行日志。
    格式使用 TAB 分隔，便于未来通用化程序直接解析。

    断点原理：
    - START 表示任务开始。
    - DONE 表示任务成功完成。
    - ERROR 表示失败。
    下一次启动时，只要没有对应 DONE，就可以重新执行。
    """
    LOGS.mkdir(parents=True, exist_ok=True)
    line = (
        f"{now()}\t{event}\t{stage}\t{task}\t"
        f"{source}\t{output}\t{detail}\n"
    )
    with _log_lock:
        with open(PIPELINE_LOG, "a", encoding="utf-8") as f:
            f.write(line)


# 函数：console
def console(msg, level="INFO"):
    prefix = {
        "INFO": "[INFO]",
        "STEP": "[STEP]",
        "DONE": "[DONE]",
        "WARN": "[WARN]",
        "ERROR": "[ERROR]",
        "SEARCH": "[SEARCH]",
        "LLM": "[LLM]",
        "PIPE": "[PIPELINE]",
    }.get(level, "[INFO]")
    print(f"{prefix} {msg}", flush=True)


# 函数：configure_logging
def configure_logging():
    LOGS.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=str(ERROR_LOG),
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        encoding="utf-8",
    )


# ============================================================
# 6. 配置与外部画像
#
# 设计原则：
# 1. Information-Gatherer.py 不保存任何用户姓名、学校、成绩、兴趣、API Key等私人参数。
# 2. set.json 保存“程序怎么运行”和“研究什么”，profile.txt 保存“是谁”。
# 3. 未来开源时，只发布 public 版 set.json/profile.txt 模板即可。
# ============================================================

# 函数：init_project
def init_project():
    # 初始化只创建运行目录，并检查外部配置；所有运行参数均由 set.json 提供。
    setup_dirs()
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"找不到 {CONFIG_PATH}。请将对应的 set.json 放在 Information-Gatherer.py 同目录后再运行。"
        )

    try:
        cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        raise RuntimeError(f"set.json 解析失败：{e}")

    profile_path = ROOT / cfg.get("profile", {}).get("path", "profile.txt")
    legacy_profile = cfg.get("user_profile")
    if legacy_profile and not profile_path.exists():
        def _flatten_profile(obj, prefix=""):
            lines = []
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if str(k).startswith("#"):
                        continue
                    label = f"{prefix}{k}" if prefix else str(k)
                    lines.extend(_flatten_profile(v, label + "."))
            elif isinstance(obj, list):
                lines.append(f"{prefix[:-1]}：{', '.join(map(str, obj))}")
            else:
                lines.append(f"{prefix}：{obj}")
            return lines
        write_text(profile_path, "\n".join(_flatten_profile(legacy_profile)) + "\n")
        cfg.pop("user_profile", None)
        write_text(CONFIG_PATH, json.dumps(cfg, ensure_ascii=False, indent=2))
    elif not profile_path.exists():
        write_text(profile_path, "# 个人画像填写在此文件；公开发布时不要提交真实个人信息。\n")

    console("初始化检查完成：set.json 是唯一运行配置来源。", "DONE")
    console(f"配置文件：{CONFIG_PATH}")
    console(f"结果目录：{RESULT}")


# 函数：load_config
def load_config():
    # 只读取 set.json，不再与 Python 内置默认配置合并。
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"找不到 {CONFIG_PATH}，请先准备同目录 set.json。"
        )
    try:
        cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        raise RuntimeError(f"set.json 解析失败：{e}")
    if not isinstance(cfg, dict):
        raise RuntimeError("set.json 顶层必须是 JSON 对象。")
    return cfg


# 函数：get_profile
def get_profile(cfg):
    # profile.txt是唯一个人画像入口；set.json不再接受user_profile作为正常来源。
    rel = cfg.get("profile", {}).get("path", "profile.txt")
    p = (ROOT / rel).resolve()
    if ROOT.resolve() not in p.parents and p != ROOT.resolve():
        raise RuntimeError("profile.path必须指向程序目录或其子目录，拒绝越界读取。")
    if not p.exists():
        return "未提供个人画像。"
    return read_text(p).strip()


# 函数：validate_config
# 在真正开始搜索/调用模型前检查关键配置，避免“跑了几十分钟才发现模型名为空”。
def validate_config(cfg):
    errors = []
    if not cfg.get("research", {}).get("topics"):
        errors.append("research.topics为空：请至少填写一个研究主题。")
    for provider_name, pc in cfg.get("providers", {}).items():
        if not pc.get("enabled"):
            continue
        if not pc.get("base_url"):
            errors.append(f"providers.{provider_name}.base_url为空。")
        models = pc.get("models", [])
        if not any(m.get("enabled") and m.get("name") for m in models):
            errors.append(f"providers.{provider_name}没有启用任何模型。")
    search = cfg.get("search", {})
    if not search.get("primary_provider"):
        errors.append("search.primary_provider不能为空。")
    providers = search.get("providers", {})
    if not isinstance(providers, dict) or not providers:
        errors.append("search.providers不能为空。")
    else:
        enabled_search = 0
        for name, pc in providers.items():
            if not pc.get("enabled", False):
                continue
            enabled_search += 1
            if int(pc.get("requests_per_minute", 0)) < 1:
                errors.append(f"search.providers.{name}.requests_per_minute必须>=1。")
            if not pc.get("url"):
                errors.append(f"search.providers.{name}.url不能为空。")
            if pc.get("type") == "json_api":
                if not pc.get("response", {}).get("results_path"):
                    errors.append(f"search.providers.{name}.response.results_path不能为空。")
        if enabled_search == 0:
            errors.append("search.providers没有启用任何Provider。")
        if search.get("primary_provider") not in providers:
            errors.append("search.primary_provider不存在于search.providers。")
    if errors:
        raise RuntimeError("配置检查失败：\n- " + "\n- ".join(errors))

# ============================================================
# 7. 超时/重试：解决用户遇到的 requests ReadTimeout
# ============================================================

# 所有本地LM Studio请求共享这一把锁。
# 这是必须的：run模式会为多个主题创建多个LLM对象，如果每个对象
# 自己有一把锁，那么不同对象之间仍然可能同时调用本地模型。
LOCAL_LLM_GLOBAL_LOCK = threading.Lock()

# 本地模型动态并发调度器：待处理请求少于阈值时使用normal_workers，
# 超过阈值后逐步提高到max_workers。当前默认1 -> 2，阈值10。
LOCAL_LLM_SCHEDULER_LOCK = threading.Lock()
LOCAL_LLM_ACTIVE = 0
LOCAL_LLM_PENDING = 0
LOCAL_LLM_CONDITION = threading.Condition(LOCAL_LLM_SCHEDULER_LOCK)

# 函数：_local_target_workers
def _local_target_workers(cfg):
    c = cfg.get("llm", {})
    normal = max(1, int(c.get("normal_workers", 1)))
    maximum = max(normal, int(c.get("max_workers", normal)))
    threshold = max(1, int(c.get("queue_threshold", 10)))
    pending = LOCAL_LLM_PENDING
    if not bool(c.get("dynamic_workers", True)):
        return normal
    if pending <= threshold:
        return normal
    # 梯级增加：每超过一个threshold区间增加一个并发槽，直到max_workers。
    step = max(1, (pending - threshold + threshold - 1) // threshold)
    return min(maximum, normal + step)

# 函数：_local_acquire
def _local_acquire(cfg):
    global LOCAL_LLM_ACTIVE, LOCAL_LLM_PENDING
    with LOCAL_LLM_CONDITION:
        LOCAL_LLM_PENDING += 1
        try:
            while LOCAL_LLM_ACTIVE >= _local_target_workers(cfg):
                LOCAL_LLM_CONDITION.wait(timeout=0.5)
            LOCAL_LLM_ACTIVE += 1
        finally:
            LOCAL_LLM_PENDING -= 1

# 函数：_local_release
def _local_release():
    global LOCAL_LLM_ACTIVE
    with LOCAL_LLM_CONDITION:
        LOCAL_LLM_ACTIVE = max(0, LOCAL_LLM_ACTIVE - 1)
        LOCAL_LLM_CONDITION.notify_all()

# 非本地Provider并发控制：每个Provider独立一个信号量，避免在线服务被多模型任务瞬间打爆。
PROVIDER_SEMAPHORES = {}
PROVIDER_SEMAPHORE_LOCK = threading.Lock()

# 函数：_provider_acquire
def _provider_acquire(cfg, provider):
    if provider == "local":
        return
    pc = cfg.get("providers", {}).get(provider, {})
    limit = max(1, int(pc.get("max_workers", cfg.get("llm", {}).get("max_workers", 10))))
    with PROVIDER_SEMAPHORE_LOCK:
        sem = PROVIDER_SEMAPHORES.get(provider)
        if sem is None or getattr(sem, "_carer_limit", None) != limit:
            sem = threading.BoundedSemaphore(limit)
            sem._carer_limit = limit
            PROVIDER_SEMAPHORES[provider] = sem
    sem.acquire()

# 函数：_provider_release
def _provider_release(cfg, provider):
    if provider == "local":
        return
    with PROVIDER_SEMAPHORE_LOCK:
        sem = PROVIDER_SEMAPHORES.get(provider)
    if sem:
        sem.release()

class LLMTimeoutError(RuntimeError):
    pass


class LLMRequestError(RuntimeError):
    pass


class LLM:
    """OpenAI兼容模型调用器。支持同一Provider下多个模型并行调用。"""

    # 函数：__init__
    def __init__(self, cfg, provider_name=None, model_cfg=None):
        self.cfg = cfg
        self.provider = provider_name or cfg.get("llm", {}).get("default_provider", "local")
        pc = cfg.get("providers", {}).get(self.provider, {})
        mc = model_cfg or {}
        self.url = (mc.get("base_url") or pc.get("base_url", "")).rstrip("/") + "/chat/completions"
        self.key = mc.get("api_key", pc.get("api_key", "lm-studio"))
        self.model = mc.get("name") or pc.get("model", "")
        self.temperature = float(mc.get("temperature", pc.get("temperature", cfg.get("llm", {}).get("temperature", 0.2))))
        self.connect_timeout = int(mc.get("connect_timeout", pc.get("connect_timeout", cfg.get("llm", {}).get("connect_timeout", 20))))
        self.read_timeout = int(mc.get("read_timeout", pc.get("read_timeout", cfg.get("llm", {}).get("read_timeout", 900))))
        self.max_tokens = int(mc.get("max_tokens", pc.get("max_tokens", cfg.get("llm", {}).get("max_tokens", 6000))))
        self.max_retries = int(cfg.get("llm", {}).get("max_retries", 1))
        self.retry_delay = float(cfg.get("llm", {}).get("retry_delay", 5))
        self.max_input_chars = int(cfg.get("llm", {}).get("max_input_chars", 36000))
        self.force_single = bool(cfg.get("llm", {}).get("force_single_thread_local", False))

    # 函数：available_models
    @staticmethod
    def available_models(cfg, selected=None):
        """展开 providers.local/online 下的所有启用模型。旧版单model字段也兼容。"""
        names = selected or cfg.get("multi_model", {}).get("providers", [])
        out = []
        for provider in names:
            pc = cfg.get("providers", {}).get(provider, {})
            if not pc.get("enabled", False):
                continue
            models = pc.get("models") or []
            if not models and pc.get("model"):
                models = [{"name": pc["model"], "enabled": True}]
            for idx, mc in enumerate(models):
                if mc.get("enabled", True) and mc.get("name"):
                    merged = dict(mc)
                    merged.setdefault("base_url", pc.get("base_url", ""))
                    merged.setdefault("api_key", pc.get("api_key", ""))
                    merged.setdefault("connect_timeout", pc.get("connect_timeout", 20))
                    merged.setdefault("read_timeout", pc.get("read_timeout", 900 if provider == "local" else 180))
                    merged.setdefault("max_tokens", pc.get("max_tokens", 6000))
                    out.append((f"{provider}[{idx}]", provider, merged))
        return out

    # 函数：chat
    def chat(self, system, user, stage="LLM", task=""):
        if not self.model:
            raise LLMRequestError(f"模型名称为空：provider={self.provider}")
        if len(user) > self.max_input_chars:
            user = user[:self.max_input_chars] + "\n\n[输入按配置截断]"
        payload = {"model": self.model, "messages":[{"role":"system","content":system},{"role":"user","content":user}],"temperature":self.temperature,"max_tokens":self.max_tokens,"stream":False}
        attempts = self.max_retries + 1
        for attempt in range(1, attempts + 1):
            console(f"{stage}：{self.provider}/{self.model} | {task or '调用模型'} | {attempt}/{attempts}", "LLM")
            event_log("START", stage, task, detail=f"model={self.provider}/{self.model};attempt={attempt}")
            try:
                # 本地模型使用统一动态调度器；多个本地模型也共享调度容量。
                acquired_local = False
                acquired_provider = False
                if self.provider == "local":
                    _local_acquire(self.cfg)
                    acquired_local = True
                else:
                    _provider_acquire(self.cfg, self.provider)
                    acquired_provider = True
                r = requests.post(self.url, headers={"Authorization":f"Bearer {self.key}","Content-Type":"application/json"}, json=payload, timeout=(self.connect_timeout,self.read_timeout))
                r.raise_for_status()
                data=r.json()
                content=data["choices"][0]["message"]["content"]
                event_log("DONE", stage, task, detail=f"model={self.provider}/{self.model}")
                console(f"{stage}：完成 | {self.provider}/{self.model}", "DONE")
                return content
            except requests.exceptions.Timeout as e:
                msg=f"LLM超时：{self.provider}/{self.model} connect={self.connect_timeout}s read={self.read_timeout}s：{e}"
                logging.error(msg); event_log("ERROR",stage,task,detail=msg); console(msg,"ERROR")
                if attempt == attempts: raise LLMTimeoutError(msg) from e
                time.sleep(self.retry_delay)
            except (requests.exceptions.RequestException,ValueError,KeyError) as e:
                msg=f"LLM请求失败：{self.provider}/{self.model}：{e}"
                logging.error(msg); event_log("ERROR",stage,task,detail=msg); console(msg,"ERROR")
                if attempt == attempts: raise LLMRequestError(msg) from e
                time.sleep(self.retry_delay)
            finally:
                if self.provider == "local":
                    _local_release()
                else:
                    _provider_release(self.cfg, self.provider)

    # 函数：chat_multi
    def chat_multi(self, system, user, stage="MULTI", task=""):
        mm=self.cfg.get("multi_model", {})
        if not mm.get("enabled", False):
            return self.chat(system,user,stage,task)
        models=self.available_models(self.cfg,mm.get("providers"))
        if not models:
            return self.chat(system,user,stage,task)
        results={}; errors={}
        def run_one(item):
            label,provider,mc=item
            try:
                return label,LLM(self.cfg,provider,mc).chat(system,user,stage=f"{stage}:{label}",task=task)
            except Exception as e:
                return label,e
        if mm.get("parallel",True):
            workers=min(len(models),int(mm.get("parallel_workers",len(models))))
            with concurrent.futures.ThreadPoolExecutor(max_workers=max(1,workers)) as ex:
                fs=[ex.submit(run_one,m) for m in models]
                for f in concurrent.futures.as_completed(fs):
                    label,val=f.result()
                    (errors if isinstance(val,Exception) else results)[label]=str(val) if isinstance(val,Exception) else val
        else:
            for m in models:
                label,val=run_one(m)
                (errors if isinstance(val,Exception) else results)[label]=str(val) if isinstance(val,Exception) else val
        minimum=max(1,int(mm.get("require_min_success",1)))
        if len(results)<minimum:
            raise LLMRequestError(f"多模型成功数不足：{len(results)}/{minimum}；错误={errors}")
        if mm.get("include_model_labels",True):
            return "\n\n".join(f"【模型 {k}】\n{v}" for k,v in results.items())
        return "\n\n".join(results.values())


# ============================================================
# 8. 搜索频率限制
# ============================================================

class RateLimiter:
    """
    简单滑动窗口限流器。
    例如 requests_per_minute=10：
    任意连续60秒最多发10个“搜索请求”。

    注意：网页正文抓取不计入搜索API配额，避免网页抓取把
    AnySearch/Bing 的10请求/分钟限制一起拖慢。
    """

    # 函数：__init__
    def __init__(self, max_calls=10, period=60):
        self.max_calls = max(1, int(max_calls))
        self.period = float(period)
        self.calls = deque()
        self.lock = threading.Lock()

    # 函数：wait
    def wait(self):
        while True:
            with self.lock:
                t = time.monotonic()
                while self.calls and t - self.calls[0] >= self.period:
                    self.calls.popleft()

                if len(self.calls) < self.max_calls:
                    self.calls.append(t)
                    return

                wait_for = self.period - (t - self.calls[0]) + 0.05

            console(f"搜索API达到频率限制，等待 {wait_for:.1f}s", "WARN")
            time.sleep(max(wait_for, 0.1))


class Searcher:
    """搜索层：Provider、API地址、Key、RPM和返回字段均由 set.json 控制。"""
    _global_limiters = {}
    _global_limiters_lock = threading.Lock()

    def __init__(self, cfg):
        self.c = cfg["search"]
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.c["user_agent"]})
        self.providers = self.c["providers"]

    @classmethod
    def _limiter(cls, provider_name, rpm):
        key = (provider_name, int(rpm))
        with cls._global_limiters_lock:
            if key not in cls._global_limiters:
                cls._global_limiters[key] = RateLimiter(int(rpm), 60)
            return cls._global_limiters[key]

    def _request(self, provider_name, method, url, **kwargs):
        pc = self.providers[provider_name]
        self._limiter(provider_name, int(pc["requests_per_minute"])).wait()
        return self.session.request(method, url, **kwargs)

    @staticmethod
    def _get_path(data, path):
        cur = data
        for key in path:
            if isinstance(cur, dict): cur = cur[key]
            elif isinstance(cur, list): cur = cur[int(key)]
            else: raise KeyError(key)
        return cur

    @staticmethod
    def _format_value(value, query, max_results):
        if isinstance(value, str):
            return value.replace("{query}", query).replace("{max_results}", str(max_results))
        if isinstance(value, dict): return {k: Searcher._format_value(v, query, max_results) for k,v in value.items()}
        if isinstance(value, list): return [Searcher._format_value(v, query, max_results) for v in value]
        return value

    def _auth_headers(self, pc):
        headers = dict(pc.get("headers", {})); key = str(pc.get("api_key", "")).strip(); auth = pc.get("auth", {})
        if key and auth.get("type") == "bearer": headers[auth.get("header", "Authorization")] = f"Bearer {key}"
        elif key and auth.get("type") == "api_key": headers[auth.get("header", "X-API-Key")] = key
        return headers

    def _json_api(self, provider_name, query):
        pc = self.providers[provider_name]; max_results = int(pc["max_results"]); method = str(pc["method"]).upper()
        headers = self._auth_headers(pc); timeout=(int(pc["connect_timeout"]), int(pc["read_timeout"]))
        req = pc["request"]; payload=self._format_value(req.get("body",{}),query,max_results); params=self._format_value(req.get("params",{}),query,max_results)
        try:
            r=self._request(provider_name,method,pc["url"],headers=headers,json=payload if method not in {"GET","HEAD"} else None,params=params or None,timeout=timeout); r.raise_for_status(); data=r.json()
            results=self._get_path(data,pc["response"]["results_path"]); fields=pc["response"]["fields"]; out=[]
            for item in results:
                def field(name):
                    try: return self._get_path(item,fields[name])
                    except (KeyError,IndexError,TypeError,ValueError): return ""
                out.append({"engine":provider_name,"title":str(field("title") or ""),"url":str(field("url") or ""),"snippet":str(field("snippet") or ""),"content":str(field("content") or "")})
            return out
        except requests.exceptions.Timeout as e:
            console(f"{provider_name}超时：{e}","ERROR"); logging.error("%s timeout: %s",provider_name,e); event_log("ERROR","SEARCH",provider_name,detail=str(e)); return []
        except Exception as e:
            console(f"{provider_name}失败：{e}","WARN"); logging.warning("%s failed: %s",provider_name,e); event_log("ERROR","SEARCH",provider_name,detail=str(e)); return []

    def _bing_html(self, provider_name, query):
        pc=self.providers[provider_name]
        try:
            params={"q":query,"count":int(pc["count"]),"qs":"n","sp":"-1","lq":"0","pq":query}
            r=self._request(provider_name,"GET",pc["url"],params=params,timeout=(int(pc["connect_timeout"]),int(pc["read_timeout"]))); r.raise_for_status(); soup=BeautifulSoup(r.text,"html.parser"); out=[]
            for item in soup.find_all("li",class_="b_algo"):
                a=item.find("a")
                if not a: continue
                cap=item.find("div",class_="b_caption")
                out.append({"engine":provider_name,"title":a.get_text(" ",strip=True),"url":a.get("href",""),"snippet":cap.get_text(" ",strip=True) if cap else "","content":""})
            return out
        except requests.exceptions.Timeout as e:
            console(f"{provider_name}超时：{e}","ERROR"); logging.error("%s timeout: %s",provider_name,e); event_log("ERROR","SEARCH",provider_name,detail=str(e)); return []
        except Exception as e:
            console(f"{provider_name}失败：{e}","WARN"); logging.warning("%s failed: %s",provider_name,e); event_log("ERROR","SEARCH",provider_name,detail=str(e)); return []

    def fetch_page(self,url):
        if not url or not url.startswith(("http://","https://")): return ""
        try:
            r=self.session.get(url,timeout=int(self.c["fetch_timeout"]),allow_redirects=True); r.raise_for_status(); ctype=r.headers.get("Content-Type","")
            if "text/html" not in ctype and "application/xhtml" not in ctype: return ""
            soup=BeautifulSoup(r.text,"html.parser")
            for tag in soup(["script","style","noscript","svg","nav","footer","header"]): tag.decompose()
            main=soup.find("main") or soup.find("article") or soup.body; text=main.get_text("\n",strip=True) if main else soup.get_text("\n",strip=True)
            return re.sub(r"[ \t]{2,}"," ",re.sub(r"\n{3,}","\n\n",text))[:int(self.c["max_page_chars"])]
        except Exception as e: logging.debug("网页抓取失败 %s: %s",url,e); return ""

    def search_provider(self,name,query):
        pc=self.providers[name]
        if not pc.get("enabled",False): return []
        if pc["type"]=="json_api": return self._json_api(name,query)
        if pc["type"]=="bing_html": return self._bing_html(name,query)
        raise RuntimeError(f"未知搜索Provider类型：{pc['type']}")

    def search(self,query):
        event_log("START","SEARCH",query); console(query,"SEARCH")
        names=[self.c["primary_provider"]]+self.c.get("secondary_providers",[]); names=list(dict.fromkeys(n for n in names if n in self.providers and self.providers[n].get("enabled",False)))
        if not names: raise RuntimeError("没有启用任何搜索Provider。请检查 set.json 的 search.providers。")
        results=[]; workers=min(len(names),int(self.c["api_workers"]))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1,workers)) as ex:
            for f in concurrent.futures.as_completed([ex.submit(self.search_provider,n,query) for n in names]):
                try: results.extend(f.result())
                except Exception as e: console(f"搜索Provider失败：{e}","WARN")
        seen=set(); merged=[]
        for x in results:
            u=x.get("url","").strip(); key=u or sha(x.get("title","")+x.get("snippet",""))
            if key in seen: continue
            seen.add(key); merged.append(x)
        if self.c["fetch_pages"] and merged:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max(1,int(self.c["page_workers"]))) as ex:
                futures={ex.submit(self.fetch_page,x["url"]):x for x in merged}
                for f in concurrent.futures.as_completed(futures):
                    x=futures[f]
                    try:
                        page=f.result()
                        if page: x["content"]=page
                    except Exception: pass
        event_log("DONE","SEARCH",query,detail=f"results={len(merged)}"); console(f"搜索完成：{len(merged)} 条结果","DONE"); return merged


# 9. 文件层
# ============================================================

# 函数：raw_save
def raw_save(topic, query, results):
    day = datetime.now().strftime("%Y-%m-%d")
    folder = L0 / day / safe_name(topic)
    folder.mkdir(parents=True, exist_ok=True)
    files = []

    for i, x in enumerate(results):
        ident = sha(
            (x.get("url", "") + x.get("title", "") + x.get("content", ""))[:50000]
        )
        p = folder / f"{ident}_{i:02d}.txt"

        text = (
            f"状态：RAW\n"
            f"抓取时间：{now()}\n"
            f"主题：{topic}\n"
            f"查询：{query}\n"
            f"搜索引擎：{x.get('engine','')}\n"
            f"标题：{x.get('title','')}\n"
            f"URL：{x.get('url','')}\n"
            f"摘要：{x.get('snippet','')}\n\n"
            f"搜索正文：\n{x.get('content','')}\n\n"
            f"网页正文：\n{x.get('page_content','')}\n"
        )

        write_text(p, text)
        files.append(p)

    return files


# 函数：read_txts
def read_txts(folder, limit):
    if not folder.exists():
        return []

    files = sorted(
        folder.rglob("*.txt"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    return [(p, read_text(p)) for p in files[:int(limit)]]


# 函数：batch_join
def batch_join(items, max_chars=36000):
    chunks = []
    total = 0

    for p, t in items:
        piece = f"\n===== SOURCE FILE: {p} =====\n{t}\n"
        if total + len(piece) > max_chars:
            break
        chunks.append(piece)
        total += len(piece)

    return "".join(chunks)


# 函数：make_batches
def make_batches(items, max_items, max_chars):
    batches = []
    current = []
    chars = 0

    for item in items:
        p, t = item
        piece_len = len(t) + len(str(p)) + 100

        if current and (
            len(current) >= max_items or chars + piece_len > max_chars
        ):
            batches.append(current)
            current = []
            chars = 0

        current.append(item)
        chars += piece_len

    if current:
        batches.append(current)

    return batches


# 函数：has_done_marker
def has_done_marker(path):
    """通过文件状态头实现幂等：已经成功晋升的输入不会重复处理。"""
    try:
        first = "\n".join(read_text(path).splitlines()[:5])
        return any(x in first for x in ["状态：DELETABLE_CANDIDATE", "状态：PROMOTED", "状态：L1_DONE"])
    except Exception:
        return False


# 函数：source_key
def source_key(path):
    return sha(str(path.resolve()))


# ============================================================
# 10. L0 -> L1
# ============================================================

# 函数：extract_txt_title
def extract_txt_title(path):
    # 不调用模型：直接读取总结TXT头部；兼容“标题：xxx”和“【标题】\nxxx”两种格式。
    if not path or not Path(path).exists():
        return "未生成"
    lines = read_text(Path(path)).splitlines()[:40]
    for i, line in enumerate(lines):
        if line.startswith("标题："):
            return line.split("：", 1)[1].strip() or "无标题"
        if line.strip() == "【标题】":
            for nxt in lines[i + 1:]:
                if nxt.strip() and not nxt.startswith("【"):
                    return nxt.strip()
    return Path(path).stem


# 函数：save_l1
def save_l1(topic, query, source_path, result, llm):
    task = source_key(source_path)
    prompt = (
        current_context()
        + "\n" + stage_policy_context(llm.cfg) + "\n\n"
        + PROMPTS["extract"]
        + "\n\n原始资料：\n"
        + result
    )

    try:
        out = llm.chat(
            BASE_SYSTEM,
            prompt,
            stage="L0->L1",
            task=task
        )
    except Exception as e:
        # 失败不删除L0，下一次仍可继续。
        event_log("ERROR", "L0->L1", task, str(source_path), detail=str(e))
        return None

    ident = sha(str(source_path) + out)
    p = L1 / safe_name(topic) / f"{datetime.now():%Y%m%d}_{ident}.txt"

    text = (
        "状态：PROCESSING\n"
        "层级：L1_INTEL\n"
        f"创建时间：{now()}\n"
        f"主题：{topic}\n"
        f"来源原文件：{source_path}\n"
        f"查询：{query}\n"
        f"源文件ID：{task}\n\n"
        f"{out}\n"
    )

    write_text(p, text)
    # L1成功落盘后才给L0写完成标记；因此中断/失败不会误跳过。
    try:
        original = read_text(source_path)
        if "状态：L1_DONE" not in original:
            write_text(source_path, "状态：L1_DONE\n" + original)
    except Exception as e:
        logging.warning("L0完成标记写入失败 %s: %s", source_path, e)
    event_log("DONE", "L0->L1", task, str(source_path), str(p))
    source_time = datetime.fromtimestamp(source_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    title = extract_txt_title(p)
    console(f"L0->L1：{source_time} | {source_path.name} | -> {title} | {p.name}", "DONE")
    return p


# ============================================================
# 11. L1 -> L2
# ============================================================

# 函数：promote_l1_to_l2
def promote_l1_to_l2(cfg):
    """
    修复旧版核心问题：
    旧版把最多80个L1文件一次性塞入一个50,000字符Prompt，
    本地模型可能生成很久，最终触发 read timeout=300。

    新版：
    - 每个主题拆成多个小批次。
    - 每个批次独立写入L2。
    - 某批次失败不会影响其它批次。
    - 下一次运行会继续未完成批次。
    """
    console("L1 -> L2", "PIPE")
    llm = LLM(cfg)
    pipe = cfg["pipeline"]

    groups = {}
    all_items = read_txts(L1, pipe.get("max_l1_items_per_run", 80))

    # 已经明确成功处理的文件跳过，兼容旧版。
    for p, t in all_items:
        if "状态：DELETABLE_CANDIDATE" in t:
            continue
        cat = p.parent.name
        groups.setdefault(cat, []).append((p, t))

    outputs = []

    for cat, items in groups.items():
        batches = make_batches(
            items,
            int(pipe.get("l1_batch_items", 6)),
            int(pipe.get("l1_batch_chars", 32000))
        )

        console(f"L1主题={cat}，待处理={len(items)}，批次={len(batches)}", "STEP")

        for idx, batch in enumerate(batches, 1):
            task = f"{safe_name(cat)}_batch_{idx}"
            source = batch_join(batch, int(pipe.get("l1_batch_chars", 32000)))
            if not source:
                continue

            try:
                if "L1->L2" in cfg.get("multi_model", {}).get("use_for", []):
                    out = llm.chat_multi(
                        current_context() + "\n" + stage_policy_context(cfg) + "\n\n" + PROMPTS["fact"],
                        source,
                        stage="L1->L2",
                        task=task
                    )
                else:
                    out = llm.chat(
                        current_context() + "\n" + stage_policy_context(cfg) + "\n\n" + PROMPTS["fact"],
                        source,
                        stage="L1->L2",
                        task=task
                    )
            except Exception as e:
                console(f"{task} 失败；保留L1，下一次可继续：{e}", "ERROR")
                continue

            ident = sha(cat + source + out)
            p = L2 / safe_name(cat) / f"{datetime.now():%Y%m%d_%H%M%S}_{ident}.txt"

            source_list = "\n".join(str(x[0]) for x in batch)
            text = (
                "状态：PROMOTED\n"
                "层级：L2_FACT\n"
                f"创建时间：{now()}\n"
                f"主题：{cat}\n"
                f"输入文件数：{len(batch)}\n"
                "输入文件：\n"
                f"{source_list}\n\n"
                f"{out}\n"
            )

            write_text(p, text)
            outputs.append(p)

            # 只有L2成功写入后，才标记对应L1为可回收。
            for src, _ in batch:
                try:
                    original = read_text(src)
                    if "状态：DELETABLE_CANDIDATE" not in original:
                        write_text(src, "状态：DELETABLE_CANDIDATE\n" + original)
                except Exception:
                    pass

            event_log(
                "DONE",
                "L1->L2",
                task,
                source_list,
                str(p),
                detail=f"batch={idx}/{len(batches)}"
            )
            for src, _ in batch:
                src_time = datetime.fromtimestamp(src.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                console(f"L1->L2 已处理：{src_time} | {src.name}", "INFO")
            console(f"L1->L2完成：{cat} 批次 {idx}/{len(batches)} | 输出标题：{extract_txt_title(p)}", "DONE")

    return outputs


# ============================================================
# 12. L2 -> L3
# ============================================================

# 函数：promote_l2_to_l3
def promote_l2_to_l3(cfg):
    console("L2 -> L3", "PIPE")
    llm = LLM(cfg)
    pipe = cfg["pipeline"]

    groups = {}
    all_items = read_txts(L2, pipe.get("max_l2_items_per_run", 40))

    for p, t in all_items:
        if "状态：DELETABLE_CANDIDATE" in t:
            continue
        cat = p.parent.name
        groups.setdefault(cat, []).append((p, t))

    outputs = []

    for cat, items in groups.items():
        batches = make_batches(
            items,
            int(pipe.get("l2_batch_items", 4)),
            int(pipe.get("l2_batch_chars", 36000))
        )

        console(f"L2主题={cat}，待处理={len(items)}，批次={len(batches)}", "STEP")

        for idx, batch in enumerate(batches, 1):
            task = f"{safe_name(cat)}_batch_{idx}"
            source = batch_join(batch, int(pipe.get("l2_batch_chars", 36000)))

            try:
                out = llm.chat(
                    current_context() + "\n" + stage_policy_context(cfg) + "\n\n" + PROMPTS["knowledge"],
                    source,
                    stage="L2->L3",
                    task=task
                )

                if "L2->L3-AUDIT" in cfg.get("multi_model", {}).get("use_for", []):
                    audit = llm.chat_multi(
                        current_context() + "\n" + stage_policy_context(cfg) + "\n\n" + PROMPTS["devil"],
                        out,
                        stage="L2->L3-AUDIT",
                        task=task
                    )
                else:
                    audit = llm.chat(
                        current_context() + "\n" + stage_policy_context(cfg) + "\n\n" + PROMPTS["devil"],
                        out,
                        stage="L2->L3-AUDIT",
                        task=task
                    )

            except Exception as e:
                console(f"{task} 失败；保留L2，下一次可继续：{e}", "ERROR")
                continue

            ident = sha(cat + source + out + audit)
            p = L3 / safe_name(cat) / f"{datetime.now():%Y%m%d_%H%M%S}_{ident}.txt"

            source_list = "\n".join(str(x[0]) for x in batch)
            text = (
                "状态：VERIFIED\n"
                "层级：L3_KNOWLEDGE\n"
                f"创建时间：{now()}\n"
                f"主题：{cat}\n"
                f"输入文件数：{len(batch)}\n"
                "输入文件：\n"
                f"{source_list}\n\n"
                "【知识提炼】\n"
                f"{out}\n\n"
                "【反方审计】\n"
                f"{audit}\n"
            )

            write_text(p, text)
            outputs.append(p)

            for src, _ in batch:
                try:
                    original = read_text(src)
                    if "状态：DELETABLE_CANDIDATE" not in original:
                        write_text(src, "状态：DELETABLE_CANDIDATE\n" + original)
                except Exception:
                    pass

            event_log(
                "DONE",
                "L2->L3",
                task,
                source_list,
                str(p),
                detail=f"batch={idx}/{len(batches)}"
            )
            for src, _ in batch:
                src_time = datetime.fromtimestamp(src.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                console(f"L1->L2 已处理：{src_time} | {src.name}", "INFO")
            console(f"L1->L2完成：{cat} 批次 {idx}/{len(batches)} | 输出标题：{extract_txt_title(p)}", "DONE")

    return outputs


# ============================================================
# 13. L3 -> COMMAND
# ============================================================

# 函数：build_command
# 把L3知识汇总成最终用户可读报告；名称保留COMMAND以兼容旧目录。
def build_command(cfg):
    console("L3 -> COMMAND", "PIPE")
    profile=get_profile(cfg)
    pipe=cfg["pipeline"]
    l3_items=read_txts(L3,pipe.get("max_l3_items_per_run",20))
    knowledge=batch_join(l3_items,int(pipe.get("l3_batch_chars",42000)))
    if not knowledge:
        console("没有可用于最终报告的L3知识。","WARN"); return None
    goal=cfg.get("research",{}).get("user_goal","")
    user=f"""{current_context()}\n\n【研究目标】\n{goal}\n\n【个人画像（如与研究相关才使用）】\n{profile}\n\n【L3知识】\n{knowledge}\n\n请生成当前研究报告。不要把研究主题强行解释成职业规划；只有研究目标明确要求时才提出行动。所有事实、趋势、推测、建议必须分开。"""
    try:
        if "REPORT" in cfg.get("multi_model",{}).get("use_for",[]):
            out=LLM(cfg).chat_multi(PROMPTS["decision"],user,"L3->COMMAND","current_report")
        else:
            out=LLM(cfg).chat(PROMPTS["decision"],user,"L3->COMMAND","current_report")
    except Exception as e:
        console(f"COMMAND生成失败：{e}","ERROR"); return None
    current=COMMAND/"当前研究报告.txt"; history=COMMAND/"历史"/f"{datetime.now():%Y-%m-%d_%H%M%S}.txt"
    content=f"层级：COMMAND\n更新时间：{now()}\n研究目标：{goal}\n\n{out}\n"
    write_text(current,content); write_text(history,content)
    event_log("DONE","L3->COMMAND","current_report","",str(current))
    console(f"COMMAND已更新：{current} | 标题：{extract_txt_title(current)}","DONE")
    return current


# ============================================================
# 14. 搜索主题
# ============================================================

# 函数：search_topic
def search_topic(cfg, topic):
    # 通用单主题搜索：topic只是用户指定的研究对象，不再假定它属于职业领域。
    searcher=Searcher(cfg)
    templates=cfg.get("research",{}).get("query_templates",[])
    count=int(cfg.get("pipeline",{}).get("queries_per_topic",len(templates) or 1))
    templates=templates[:count] or ["{topic} {year}"]
    all_results=[]
    year=datetime.now().year
    extra=cfg.get("research",{}).get("extra_search_instructions","").strip()
    profile_hint=""
    if cfg.get("profile",{}).get("inject_into_search",False):
        profile_hint=get_profile(cfg).strip()
    for i,tpl in enumerate(templates,1):
        try:
            q=tpl.format(topic=topic,year=year)
        except KeyError:
            q=tpl.replace("{topic}",topic).replace("{year}",str(year))
        if extra and len(q)<500:
            q=f"{q} {extra}"
        # 只有用户显式打开inject_into_search时才把profile作为搜索上下文；默认关闭，防止隐私泄露。
        if profile_hint and len(q)<1000:
            q=f"{q} {profile_hint[:800]}"
        console(f"{topic}：查询 {i}/{len(templates)}：{q}","STEP")
        event_log("START","SEARCH_QUERY",topic,detail=q)
        results=searcher.search(q)
        all_results.extend(results[:int(cfg["pipeline"].get("results_per_query",8))])
        event_log("DONE","SEARCH_QUERY",topic,detail=f"query={q};results={len(results)}")
        time.sleep(float(cfg["pipeline"].get("sleep_between_search_queries",0.5)))
    seen=set();unique=[]
    for x in all_results:
        key=x.get("url") or sha(x.get("title","")+x.get("snippet",""))
        if key not in seen:
            seen.add(key);unique.append(x)
    raw_files=raw_save(topic,"多查询",unique)
    event_log("DONE","SEARCH_SAVE",topic,detail=f"raw={len(raw_files)}")
    console(f"{topic}：L0保存 {len(raw_files)} 条", "DONE")
    return raw_files

# 函数：process_one_l0_to_l1
def process_one_l0_to_l1(cfg, raw_path):
    # 单个L0->L1任务：独立日志、独立异常、独立输出，保证断点续接。
    topic = raw_path.parent.name
    llm = LLM(cfg)
    return save_l1(topic, "多查询", raw_path, read_text(raw_path), llm)

# 函数：promote_l0_to_l1
def promote_l0_to_l1(cfg, raw_files):
    # 使用线程池制造真实的待处理队列，让LM Studio动态并发调度器可以按积压量工作。
    workers = max(1, int(cfg.get("search", {}).get("api_workers", 10)))
    pending = [p for p in raw_files if not has_done_marker(p)]
    console(f"L0 -> L1：待处理 {len(pending)} 条", "PIPE")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(process_one_l0_to_l1, cfg, p): p for p in pending}
        for f in concurrent.futures.as_completed(futures):
            p = futures[f]
            try:
                out = f.result()
                title = extract_txt_title(out) if out else "失败（保留L0）"
                console(f"L0->L1：{p.name} -> {title}", "DONE" if out else "ERROR")
            except Exception as e:
                event_log("ERROR", "L0->L1", str(p), str(p), detail=str(e))
                console(f"L0->L1失败：{p.name}：{e}", "ERROR")

    return pending


# ============================================================
# 15. 清理
# ============================================================

# 函数：archive_and_delete
def archive_and_delete(p, layer):
    rel = p.relative_to(RESULT)
    target = ARCHIVE / datetime.now().strftime("%Y-%m") / layer / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(p, target)
    p.unlink()
    event_log("DELETE", "CLEANUP", str(p), str(p), str(target))


# 函数：cleanup
def cleanup(cfg):
    # 清理只在enable_delete=True时真正删除；默认只提示，不碰用户数据。
    # ARCHIVE是“删除前备份区”，不是正常信息层。开启archive_enabled后才会使用。
    pipe = cfg["pipeline"]
    if not pipe.get("enable_delete", False):
        console("自动删除未启用：本轮不删除、不移动任何TXT。", "INFO")
        return

    archive_enabled = bool(pipe.get("archive_enabled", True))
    now_dt = datetime.now()
    l0_days = int(pipe.get("l0_delete_days", 7))
    delay = int(pipe.get("promoted_delete_days", 3))

    for p in list(L0.rglob("*.txt")):
        try:
            if has_done_marker(p):
                age = (now_dt - datetime.fromtimestamp(p.stat().st_mtime)).days
                if age >= l0_days:
                    if archive_enabled:
                        archive_and_delete(p, "L0")
                    else:
                        p.unlink()
                        event_log("DELETE", "CLEANUP", str(p), str(p), detail="archive_disabled")
        except Exception as e:
            logging.warning("L0清理失败 %s: %s", p, e)

    for layer_name, folder in [("L1", L1), ("L2", L2), ("L3", L3)]:
        for p in list(folder.rglob("*.txt")):
            try:
                text = read_text(p)
                if "DELETABLE_CANDIDATE" not in text:
                    continue
                age = (now_dt - datetime.fromtimestamp(p.stat().st_mtime)).days
                if age >= delay:
                    if archive_enabled:
                        archive_and_delete(p, layer_name)
                    else:
                        p.unlink()
                        event_log("DELETE", "CLEANUP", str(p), str(p), detail="archive_disabled")
            except Exception as e:
                logging.warning("清理失败 %s: %s", p, e)


# ============================================================
# 16. 报告
# ============================================================

# 函数：generate_report
def generate_report(cfg, period):
    llm = LLM(cfg)
    profile = get_profile(cfg)

    command_path = COMMAND / "当前总战略.txt"
    command = read_text(command_path) if command_path.exists() else ""

    prompts = {
        "daily": "生成今日情报：只报告过去7天真正影响用户的变化；最后给出最多3项行动。",
        "weekly": "生成本周报告：总结新增情报、路线变化、机会、风险和下周3项行动。",
        "monthly": "生成本月战略报告：比较本月与上月路线排名、竞争力变化、重要机会、风险和下月行动。"
    }

    user = f"""
{current_context()}
{stage_policy_context(cfg)}

个人画像：
{profile}

当前COMMAND：
{command}

任务：
{prompts[period]}
"""

    try:
        if "REPORT" in cfg.get("multi_model", {}).get("use_for", []):
            out = llm.chat_multi(
                BASE_SYSTEM,
                user,
                stage=f"REPORT-{period}",
                task=period
            )
        else:
            out = llm.chat(
                BASE_SYSTEM,
                user,
                stage=f"REPORT-{period}",
                task=period
            )
    except Exception as e:
        console(f"{period}报告生成失败：{e}", "ERROR")
        return None

    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    p = REPORTS / period / f"{stamp}.txt"
    write_text(
        p,
        f"报告类型：{period}\n生成时间：{now()}\n\n{out}\n"
    )

    event_log("DONE", f"REPORT-{period}", period, "", str(p))
    console(f"{period}报告完成：{p}", "DONE")
    return p


# ============================================================
# 17. 完整流水线
# ============================================================

# 函数：run_pipeline
def run_pipeline(cfg):
    validate_config(cfg)
    # 完整运行顺序：先搜索全部主题，再集中L0->L1，再逐层晋升。
    # 这样搜索队列能形成积压，动态LM Studio并发才有意义，同时各层状态更容易断点恢复。
    setup_dirs(cfg)
    topics = cfg.get("research", {}).get("topics", [])
    search_workers = max(1, int(cfg.get("search", {}).get("api_workers", 10)))
    topic_workers = min(search_workers, max(1, len(topics)))
    console(f"开始完整运行：主题={len(topics)}，搜索线程={topic_workers}", "PIPE")
    event_log("START", "RUN", "full_pipeline", detail=f"topics={len(topics)}")

    all_raw = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=topic_workers) as ex:
        futures = {ex.submit(search_topic, cfg, topic): topic for topic in topics}
        for f in concurrent.futures.as_completed(futures):
            topic = futures[f]
            try:
                files = f.result()
                all_raw.extend(files)
                console(f"搜索完成：{topic}，L0={len(files)}", "DONE")
            except Exception as e:
                logging.exception("主题任务失败：%s", topic)
                event_log("ERROR", "SEARCH_TOPIC", topic, detail=str(e))
                console(f"主题失败：{topic}：{e}", "ERROR")

    promote_l0_to_l1(cfg, all_raw)
    promote_l1_to_l2(cfg)
    promote_l2_to_l3(cfg)
    build_command(cfg)
    cleanup(cfg)
    event_log("DONE", "RUN", "full_pipeline")
    console("完整流水线结束。", "DONE")


# ============================================================
# 18. 彩色帮助
#    用户要求 python Information-Gatherer.py --help 保持彩色且格式清楚。
# ============================================================

# 函数：print_colored_help
def print_colored_help(parser):
    if colorama_init:
        colorama_init()

    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RESET = "\033[0m"

    print(f"{CYAN}============================================================{RESET}")
    print(f"{GREEN} Information-Gatherer - 个人/通用信息研究系统{RESET}")
    print(f"{CYAN}============================================================{RESET}")
    print(f"{YELLOW}快速使用：{RESET}")
    print("  python Information-Gatherer.py --init")
    print("  python Information-Gatherer.py --search \"主题\"")
    print("  python Information-Gatherer.py --promote")
    print("  python Information-Gatherer.py --run")
    print("  python Information-Gatherer.py --daily")
    print("  python Information-Gatherer.py --weekly")
    print("  python Information-Gatherer.py --monthly")
    print("  python Information-Gatherer.py --cleanup")
    print("")
    print(f"{YELLOW}参数说明：{RESET}")
    print("  --init       初始化运行目录并检查外部配置")
    print("  --search     搜索一个主题并进入L0/L1")
    print("  --promote    执行 L1->L2->L3->COMMAND")
    print("  --run        完整运行：搜索+整理+晋升+COMMAND")
    print("  --daily      生成日报")
    print("  --weekly     生成周报")
    print("  --monthly    生成月报")
    print("  --cleanup    执行清理")
    print("  --help       显示本帮助")
    print("")
    print(f"{CYAN}============================================================{RESET}")
    print(f"{GREEN}详细说明（小白版）{RESET}")
    print(f"{CYAN}============================================================{RESET}")
    print("""
1. 第一次使用：
   先运行：
       python Information-Gatherer.py --init
   它会创建 RESULT、logs 等运行目录，并检查同目录 set.json/profile.txt。

2. 搜索：
       python Information-Gatherer.py --search "你的研究主题"
   搜索结果先进入 L0_RAW，再由本地AI提炼到 L1_INTEL。

3. 整理：
       python Information-Gatherer.py --promote
   按 L1 -> L2 -> L3 -> COMMAND 逐层处理。
   每一批任务都会单独记录，因此程序中途关闭，下次仍可继续。

4. 完整运行：
       python Information-Gatherer.py --run
   会完成搜索、AI整理、事实核验、知识提炼和最终输出；具体研究内容由 set.json 决定。

5. 最重要的配置：
   打开 set.json，可以修改：
   - LM Studio地址、模型、最大生成长度
   - LLM连接/读取超时
   - LLM重试次数
   - 搜索API超时
   - 搜索每分钟最大请求数
   - 搜索线程数
   - L1/L2/L3每批处理多少文件
   - 自动删除开关
   - 搜索主题和搜索模板

6. 为什么新版L1->L2不会像旧版一样卡死？
   旧版一次最多把80个L1文件拼成约5万字符，再让本地模型一次性处理。
   如果模型生成速度较慢，requests会一直等到300秒，最后触发ReadTimeout。
   新版改成小批次，每个批次独立调用模型；某一批失败不会影响其他批次。

7. 为什么日志这么重要？
   RESULT/logs/pipeline.log 每完成一个关键步骤就写一行。
   你可以随时关闭程序。下一次启动时，已经成功写入DONE的步骤不会重复处理；
   没有DONE的任务仍然保留在原层，程序可以继续。

8. 关于时间：
   程序会自动把当前日期注入搜索和AI Prompt。
   因此“2026年招聘信息”会被标记为当前信息；
   对2029/2030年的判断则要求AI结合历史趋势，不允许把今天的招聘要求当成未来确定事实。

9. 关于线程：
   搜索可以多线程，而且AnySearch/Bing共享全局“每分钟最多10个搜索请求”的限流器。
   网页正文抓取可以使用page_workers并发。
   LM Studio默认采用动态并发：待处理请求不超过10时使用normal_workers（默认1）；
   超过10后开始梯级增加，最高到max_workers（默认2）。
   如果你希望永远单线程，把force_single_thread_local改成true即可。

10. 关于旧数据：
    新版会继续使用已有L0/L1/L2/L3/COMMAND文件，不要求因程序升级而重复搜索。
    原来的L0/L1/L2/L3/COMMAND文件可以直接继续使用。
    新版会兼容旧版set.json中已有字段，缺少的新字段使用默认值。

11. 如果出现LLM超时：
    控制台会明确显示ERROR；
    RESULT/logs/errors.log会记录完整错误；
    RESULT/logs/pipeline.log会记录该步骤没有完成。
    下一次运行时不会删除失败输入，而是继续尝试。

12. 多模型：
    providers.local/providers.online分别配置本地和在线模型。
    multi_model.enabled=true后，可以让多个已启用模型并行交叉处理指定阶段；
    require_min_success控制至少几个模型成功。当前默认关闭，避免你还没有配置在线模型时产生额外请求。

13. ARCHIVE是什么：
    ARCHIVE不是新的知识层，而是“删除前备份区”。
    当enable_delete=true且archive_enabled=true时，已经成功被下一层消化、达到保留天数的旧TXT会先复制到ARCHIVE，再从原层删除。
    默认enable_delete=false，因此正常运行不会删除任何信息。

14. 当前阶段防幻觉：
    如果你需要阶段性行为约束，请把它写入set.json的research.stage_policy；程序不会内置任何人的人生阶段。

15. 未来通用化：
    本版本已经预留providers、multi_model、通用topics、query templates、stage_policy和mode_name。
    下一版本可以把它改成“输入任意目标 -> 搜索 -> 分类 -> 总结 -> 交叉验证 -> 输出”的通用研究器。
""")
    print(f"{CYAN}============================================================{RESET}")
    print(f"{GREEN}当前时间：{now()}{RESET}")
    print(f"{CYAN}============================================================{RESET}")


# ============================================================
# 19. CLI
# ============================================================

# 函数：build_parser
def build_parser():
    parser = argparse.ArgumentParser(
        description="Information-Gatherer 通用个人/信息研究系统",
        add_help=False
    )
    parser.add_argument("--init", action="store_true", help="初始化/升级配置")
    parser.add_argument("--search", metavar="TOPIC", help="只搜索一个主题")
    parser.add_argument("--promote", action="store_true", help="执行 L1->L2->L3->COMMAND")
    parser.add_argument("--run", action="store_true", help="完整运行一轮")
    parser.add_argument("--daily", action="store_true", help="生成日报")
    parser.add_argument("--weekly", action="store_true", help="生成周报")
    parser.add_argument("--monthly", action="store_true", help="生成月报")
    parser.add_argument("--cleanup", action="store_true", help="执行清理")
    parser.add_argument("-h", "--help", action="store_true", help="显示彩色使用说明")
    return parser


# 函数：main
def main():
    parser = build_parser()

    # 特意在parse_args之前拦截--help，保证输出保持彩色格式。
    if "--help" in sys.argv or "-h" in sys.argv:
        print_colored_help(parser)
        return

    args = parser.parse_args()

    if args.init:
        init_project()
        return

    cfg = load_config()
    validate_config(cfg)
    setup_dirs(cfg)
    configure_logging()

    try:
        if args.search:
            raw = search_topic(cfg, args.search)
            promote_l0_to_l1(cfg, raw)
            console(f"完成：{len(raw)} 条结果进入L0/L1", "DONE")

        elif args.promote:
            promote_l1_to_l2(cfg)
            promote_l2_to_l3(cfg)
            build_command(cfg)
            cleanup(cfg)

        elif args.run:
            run_pipeline(cfg)

        elif args.daily:
            generate_report(cfg, "daily")

        elif args.weekly:
            generate_report(cfg, "weekly")

        elif args.monthly:
            generate_report(cfg, "monthly")

        elif args.cleanup:
            cleanup(cfg)

        else:
            print_colored_help(parser)

    except KeyboardInterrupt:
        event_log("STOP", "SYSTEM", "keyboard_interrupt")
        console("检测到手动中断。已完成的步骤保留，下一次可以继续。", "WARN")

    except Exception as e:
        logging.exception("程序未处理异常")
        event_log("ERROR", "SYSTEM", "unhandled_exception", detail=str(e))
        console(f"程序发生未处理异常：{e}", "ERROR")
        console("请查看 RESULT/logs/errors.log。失败输入不会自动删除。", "WARN")


if __name__ == "__main__":
    main()