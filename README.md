# Information Gatherer

本项目是一个本地信息研究与整理工具，用于将网络搜索资料经过 AI 提取、核验和整理，最终生成可阅读的研究结果。

## 1. 环境要求

- Windows 10/11
- Python 3.10+
- 已安装项目依赖
- 如使用本地模型，需要提前启动 LM Studio 等 OpenAI Compatible API 服务

## 2. 安装

在项目目录打开终端：

```bash
pip install -r requirements.txt
```

## 3. 文件说明

```text
Information Gatherer.py   主程序
set.json                  程序配置
profile.txt               个人资料（可选）
requirements.txt          Python依赖
Start.bat                 Windows启动菜单
RESULT/                   所有研究结果（初始化后即可自动生成）
```

主要结果目录：

```text
RESULT/
├─ L0_RAW/          原始搜索资料
├─ L1_INTEL/        AI提取后的情报
├─ L2_FACT/         事实核验结果
├─ L3_KNOWLEDGE/    综合知识
├─ COMMAND/         最终决策/研究结果
├─ ARCHIVE/         已归档资料
├─ reports/         日报、周报、月报等
└─ logs/            运行日志和错误日志
```

## 4. 第一次使用

先检查 `set.json`，填写搜索 API、LLM 等配置。

如果需要使用个人资料，将内容填写到 `profile.txt`。

然后初始化：

```bash
python "Information-Gatherer.py" --init
```

## 5. 常用命令

### 查看帮助

```bash
python "Information-Gatherer.py" --help
```

### 搜索指定主题

```bash
python "Information-Gatherer.py" --search "研究主题"
```

### 执行信息晋升

```bash
python "Information-Gatherer.py" --promote
```

### 完整运行

```bash
python "Information-Gatherer.py" --run
```

### 日报

```bash
python "Information-Gatherer.py" --daily
```

### 周报

```bash
python "Information-Gatherer.py" --weekly
```

### 月报

```bash
python "Information-Gatherer.py" --monthly
```

### 清理

```bash
python "Information-Gatherer.py" --cleanup
```

也可以直接运行 `Start.bat`，通过菜单操作。

## 6. 信息处理流程

程序按照以下流程处理资料：

```text
搜索
 ↓
L0_RAW
 ↓
L1_INTEL
 ↓
L2_FACT
 ↓
L3_KNOWLEDGE
 ↓
COMMAND
```

- `L0_RAW`：原始搜索资料
- `L1_INTEL`：AI提取信息
- `L2_FACT`：事实核验
- `L3_KNOWLEDGE`：综合分析
- `COMMAND`：最终研究/决策结果

## 7. 中断与继续

程序不要求持续运行，可以随时关闭。

已经成功保存的结果不会因为程序关闭而全部重新开始。日志位于：

```text
RESULT/logs/
```

## 8. 超时

如果本地模型处理速度较慢，可以在 `set.json` 中增加 LLM 读取超时时间；如果仍然超时，可以降低单批次处理的数据量。

失败任务会记录到：

```text
RESULT/logs/errors.log
```

## 9. 配置与隐私

程序运行参数放在 `set.json` 中，个人资料放在 `profile.txt` 中。

不要将包含真实 API Key 或私人资料的配置文件上传到公开仓库。

## 10. 主程序文件名

默认主程序命名为 `Information-Gatherer.py`，由于文件名不包含空格，命令行运行时可不加引号：
```bash
python Information-Gatherer.py --run
```

如果更改主程序命名为 `Information Gatherer.py`，由于文件名包含空格，命令行运行时必须加引号：
```bash
python "Information Gatherer.py" --run
```

`Start.bat` 也需要使用对应的新文件名。