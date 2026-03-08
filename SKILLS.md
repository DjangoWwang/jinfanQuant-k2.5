# Claude Code Skills

FOF 平台开发用到的自定义 Claude Code Skills。

## triple-review

**三方代码评审自动化 skill**

- 并行调用 Codex (GPT-5.4) + MiniMax (M1) + Kimi 三个 LLM 进行代码评审
- 支持中文和英文命令调用
- 自动保存评审结果为 markdown 报告
- 支持多轮迭代（R1, R2, R3...）

**安装：**
```bash
cp ~/.claude/skills/triple-review.skill ~/.claude/skills/
# 或直接解压 .skill 文件到 ~/.claude/skills/triple-review/
```

**使用：**
```bash
# 英文
triple-review _p3_5_review_prompt.txt p3_5 R1

# 中文
三方评审 _p3_5_review_prompt.txt p3_5 R1
```

**路径：** `~/.claude/skills/triple-review.skill` (或 `~/.claude/skills/triple-review/`)

更多信息见 skill 内 SKILL.md。
