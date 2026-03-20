"""Structured finance and quantitative trading curriculum for learners."""

from __future__ import annotations

from copy import deepcopy
from typing import Any


def _entry(term: str, summary: str, why_it_matters: str, keywords: list[str] | None = None) -> dict[str, Any]:
    """Build one knowledge-base entry."""

    return {
        "term": term,
        "summary": summary,
        "why_it_matters": why_it_matters,
        "keywords": keywords or [],
    }


def _section(category: str, description: str, entries: list[dict[str, Any]], keywords: list[str] | None = None) -> dict[str, Any]:
    """Build one knowledge-base section."""

    return {
        "category": category,
        "description": description,
        "entries": entries,
        "keywords": keywords or [],
    }


class LearningHubService:
    """Serve a finance-first learning hub with a broad knowledge base and guided lessons."""

    def __init__(self) -> None:
        self._hub = self._build_hub()
        self._quiz_answer_key = {
            item["question_id"]: {
                "correct_option_id": item["correct_option_id"],
                "explanation": item["explanation"],
                "lesson_id": item["lesson_id"],
            }
            for item in self._hub["quiz"]["questions"]
        }

    def hub_payload(self) -> dict[str, Any]:
        """Return the public learning hub payload without exposing quiz answers."""

        payload = deepcopy(self._hub)
        for question in payload["quiz"]["questions"]:
            question.pop("correct_option_id", None)
            question.pop("explanation", None)
        return payload

    def evaluate_quiz(self, answers: dict[str, str] | None) -> dict[str, Any]:
        """Score one submitted quiz answer set and return structured feedback."""

        if not isinstance(answers, dict):
            answers = {}
        results: list[dict[str, Any]] = []
        weak_lessons: set[str] = set()
        correct_count = 0
        for question in self._hub["quiz"]["questions"]:
            question_id = question["question_id"]
            selected_option_id = answers.get(question_id)
            answer_key = self._quiz_answer_key[question_id]
            correct_option_id = answer_key["correct_option_id"]
            is_correct = selected_option_id == correct_option_id
            if is_correct:
                correct_count += 1
            else:
                weak_lessons.add(answer_key["lesson_id"])
            results.append(
                {
                    "question_id": question_id,
                    "prompt": question["prompt"],
                    "selected_option_id": selected_option_id,
                    "correct_option_id": correct_option_id,
                    "is_correct": is_correct,
                    "explanation": answer_key["explanation"],
                    "lesson_id": answer_key["lesson_id"],
                }
            )
        total_questions = len(self._hub["quiz"]["questions"])
        score = round((correct_count / total_questions) * 100) if total_questions else 0
        pass_score = int(self._hub["quiz"]["pass_score"])
        recommendations = self._recommend_next_steps(score, weak_lessons)
        return {
            "title": self._hub["quiz"]["title"],
            "score": score,
            "pass_score": pass_score,
            "passed": score >= pass_score,
            "correct_count": correct_count,
            "total_questions": total_questions,
            "results": results,
            "weak_lessons": [lesson for lesson in self._hub["lessons"] if lesson["lesson_id"] in weak_lessons],
            "recommended_next_steps": recommendations,
        }

    def _recommend_next_steps(self, score: int, weak_lessons: set[str]) -> list[str]:
        """Build concise follow-up suggestions based on quiz performance."""

        if score >= 90:
            return [
                "继续把知识库里的术语映射到当前工作台功能，例如把货币、估值、执行、风控概念对应到具体页面。",
                "在模拟交易页做一次完整的研究 -> 下单 -> 跟踪 -> 复盘闭环，再用错题章节补短板。",
            ]
        suggestions = [
            "先按知识库重新梳理一遍宏观、资产、交易、风控和机构体系的主线，确保不是孤立记概念。",
        ]
        if weak_lessons:
            titles = [
                lesson["title"]
                for lesson in self._hub["lessons"]
                if lesson["lesson_id"] in weak_lessons
            ]
            suggestions.append(f"优先复习这些章节：{'、'.join(titles)}。")
        if score < 70:
            suggestions.append("建议先重新走一遍学习计划前四个阶段，再做下一次测验。")
        else:
            suggestions.append("复盘错题后可以直接再测一次，目标先稳定达到 85 分以上。")
        return suggestions

    def _build_hub(self) -> dict[str, Any]:
        """Build the full learning hub content."""

        knowledge_base = self._build_knowledge_base()
        lessons = self._build_lessons()
        total_entries = sum(len(section["entries"]) for section in knowledge_base)
        return {
            "overview": {
                "title": "金融与量化全景学习中心",
                "subtitle": "覆盖宏观、货币、市场、股票、期货、区块链、交易、银行、保险、监管与量化研究。",
                "audience": "适合金融、财经与量化新手，也适合想系统补基础的研究员、交易员和产品同学。",
                "outcome": "完成后应能搭出完整的金融知识地图，看懂大部分金融与量化术语，理解策略从宏观背景到执行落地的基本框架。",
                "knowledge_section_count": len(knowledge_base),
                "knowledge_entry_count": total_entries,
                "coverage_domains": [
                    "宏观经济",
                    "货币与利率",
                    "股票与估值",
                    "债券与信用",
                    "期货与期权",
                    "区块链与数字资产",
                    "交易执行",
                    "量化研究",
                    "风险管理",
                    "银行",
                    "保险",
                    "监管与基础设施",
                ],
                "steps": [
                    {"step": 1, "title": "知识地图", "description": "先把财经与金融知识按主题连成图，而不是零散记术语。"},
                    {"step": 2, "title": "阶段计划", "description": "按宏观、资产、交易、机构与量化顺序推进。"},
                    {"step": 3, "title": "课程学习", "description": "每节课都包含目标、核心内容和课后练习。"},
                    {"step": 4, "title": "测验校验", "description": "通过测验和错题反馈确认是否真正理解。"},
                ],
            },
            "knowledge_base": knowledge_base,
            "study_plan": self._build_study_plan(),
            "lessons": lessons,
            "quiz": self._build_quiz(),
        }

    def _build_knowledge_base(self) -> list[dict[str, Any]]:
        """Build the broad knowledge-base taxonomy."""

        return [
            _section(
                "宏观经济与财经周期",
                "理解经济增长、通胀、就业和政策周期，是读懂市场环境的起点。",
                [
                    _entry("GDP", "国内生产总值，用来衡量一个经济体在一定时期内创造的总产出。", "市场会用它判断经济是扩张还是放缓。"),
                    _entry("通货膨胀", "整体物价水平持续上升，意味着货币购买力下降。", "通胀会直接影响利率、估值和居民消费。"),
                    _entry("失业率", "劳动力中处于失业状态的人口比例。", "它能帮助判断经济景气度和政策宽松空间。"),
                    _entry("经济周期", "经济在复苏、过热、滞胀和衰退之间往复切换。", "不同阶段往往对应不同资产表现。"),
                    _entry("领先指标 / 滞后指标", "领先指标更早反映经济变化，滞后指标更多确认趋势。", "做资产配置时不能只看一种指标。"),
                    _entry("财政政策", "政府通过税收、赤字和财政支出来调节经济。", "它会影响基建、消费、信用和企业盈利。"),
                ],
                keywords=["宏观", "财经", "经济", "周期"],
            ),
            _section(
                "货币、利率与汇率",
                "货币体系决定资金价格，利率和汇率是大部分资产定价的底层变量。",
                [
                    _entry("中央银行", "负责货币政策、流动性管理和金融稳定的核心机构。", "央行政策会影响市场流动性和风险偏好。"),
                    _entry("货币供应量", "市场中可流通货币和近似货币的存量，例如 M1、M2。", "它反映金融条件和信用扩张节奏。"),
                    _entry("政策利率", "央行设定或引导的基准资金价格。", "它会传导到债券收益率、贷款成本和股票估值。"),
                    _entry("收益率曲线", "不同期限利率组成的曲线。", "曲线形态常被用来观察经济预期和衰退风险。"),
                    _entry("汇率", "一种货币相对另一种货币的价格。", "跨市场投资、进出口和资本流动都离不开汇率。"),
                    _entry("套息交易", "借低息货币、买高息资产或高息货币的策略。", "它解释了部分外汇和跨资产资金流逻辑。"),
                ],
                keywords=["货币", "利率", "汇率", "外汇"],
            ),
            _section(
                "金融市场与交易场所",
                "先搞清楚市场是怎么组织的，才能理解报价、成交和监管边界。",
                [
                    _entry("一级市场", "资产首次发行和融资的市场。", "公司上市、债券发行都发生在一级市场。"),
                    _entry("二级市场", "投资者之间转让已发行资产的市场。", "量化交易主要发生在二级市场。"),
                    _entry("交易所", "提供撮合、信息披露和规则约束的集中交易场所。", "不同交易所的交易制度决定了策略约束。"),
                    _entry("经纪商 / 券商", "为客户提供交易通道、融资融券、研究和托管等服务。", "策略落地离不开经纪商接口与清算链路。"),
                    _entry("做市商", "持续提供买卖报价并承担流动性供给角色的参与者。", "它影响点差、成交深度和执行成本。"),
                    _entry("清算与结算", "成交后完成头寸确认、资金证券交收的过程。", "研究结果要变成真实收益，必须经过清算结算。"),
                ],
                keywords=["市场", "交易所", "券商", "基础设施"],
            ),
            _section(
                "股票、行业与公司分析",
                "股票研究既要看企业，也要看行业结构和资本市场定价。",
                [
                    _entry("股票", "代表公司所有权份额的权益工具。", "股票是研究最广泛、信息最丰富的资产类别之一。"),
                    _entry("市值", "公司股价乘以总股本得到的市场价值。", "它影响指数权重、流动性和因子分层。"),
                    _entry("行业与板块", "把公司按业务模式和产业链位置进行归类。", "选股和风险分散都离不开行业视角。"),
                    _entry("护城河", "企业维持高回报率和竞争优势的能力。", "长期投资往往比短期情绪更依赖企业竞争力。"),
                    _entry("分红", "公司把利润的一部分返还给股东。", "分红关系到股东回报和现金流质量判断。"),
                    _entry("公司行为", "拆股、送股、回购、配股、停复牌等事件。", "它们会直接影响价格序列和回测口径。"),
                ],
                keywords=["股票", "行业", "公司", "企业"],
            ),
            _section(
                "财务报表、现金流与估值",
                "企业分析的核心不是只看利润，而是看利润质量、现金流和估值框架。",
                [
                    _entry("利润表", "反映收入、成本和利润形成过程的财务报表。", "它帮助判断企业盈利能力和增长质量。"),
                    _entry("资产负债表", "反映企业在某一时点资产、负债和所有者权益结构。", "它能揭示杠杆、偿债压力和资本结构。"),
                    _entry("现金流量表", "反映经营、投资和融资现金流入流出。", "现金流常常比利润更难被粉饰。"),
                    _entry("ROE", "净资产收益率，衡量股东投入资本的盈利效率。", "高质量公司通常能长期维持较高 ROE。"),
                    _entry("PE / PB / EVEBITDA", "常见估值指标，用来衡量股价相对盈利、净资产或经营现金流的水平。", "估值决定了好公司是不是好价格。"),
                    _entry("DCF", "折现现金流估值方法，把未来现金流折现成当前价值。", "它提醒投资者估值最终还是回到现金流。"),
                ],
                keywords=["财报", "估值", "现金流", "基本面"],
            ),
            _section(
                "债券与固定收益",
                "理解债券、信用和利率，是金融知识完整度的重要一环。",
                [
                    _entry("债券", "发行人承诺按约定支付利息并归还本金的债务工具。", "债券是利率定价和资产配置的重要基石。"),
                    _entry("到期收益率", "持有债券到到期时的综合收益率。", "它是比较不同债券吸引力的核心指标。"),
                    _entry("久期", "衡量债券价格对利率变化敏感度的指标。", "利率一动，久期越长的债券波动越大。"),
                    _entry("信用利差", "信用债收益率相对无风险利率的额外补偿。", "它反映市场对违约风险和流动性风险的定价。"),
                    _entry("回购", "以证券作抵押进行短期融资的交易。", "回购市场是金融机构流动性管理的重要枢纽。"),
                    _entry("违约风险", "债务人无法按时兑付利息或本金的风险。", "信用研究和压力测试都离不开它。"),
                ],
                keywords=["债券", "固定收益", "信用", "利差"],
            ),
            _section(
                "期货、期权与衍生品",
                "衍生品把杠杆、期限和非线性收益带进交易世界，也带来更高复杂度。",
                [
                    _entry("期货", "约定未来某一时间按约定价格交割标的物的标准化合约。", "期货广泛用于对冲、投机和价格发现。"),
                    _entry("保证金", "交易所或经纪商要求的履约担保资金。", "杠杆越高，保证金与风控越重要。"),
                    _entry("基差", "现货价格与期货价格之间的差值。", "基差变化是套保、套利和展期研究的核心。"),
                    _entry("正向市场 / 反向市场", "期货远月价格高于或低于近月价格的结构。", "它影响商品、期货 ETF 和滚动收益。"),
                    _entry("期权希腊值", "Delta、Gamma、Theta、Vega 等衡量期权风险敞口的指标。", "不会看希腊值，很难真正管理期权仓位。"),
                    _entry("隐含波动率", "市场通过期权价格反推出来的预期波动。", "它是期权定价、事件交易和情绪判断的重要输入。"),
                ],
                keywords=["期货", "期权", "衍生品", "杠杆"],
            ),
            _section(
                "区块链与数字资产",
                "数字资产市场有自己的技术栈、清算逻辑和风险结构。",
                [
                    _entry("区块链", "通过分布式账本记录交易并保持共识的数据结构与网络。", "理解链本身，才能理解链上资产与应用。"),
                    _entry("共识机制", "网络节点就账本状态达成一致的方法，如 PoW、PoS。", "它决定安全性、性能和代币激励结构。"),
                    _entry("智能合约", "部署在链上的可自动执行程序。", "DeFi、稳定币和链上交易都依赖智能合约。"),
                    _entry("稳定币", "锚定法币或其他资产价值的数字代币。", "它是数字资产市场中的结算媒介和流动性核心。"),
                    _entry("DeFi", "以智能合约为基础的去中心化金融应用集合。", "它把借贷、交易、质押和做市搬到了链上。"),
                    _entry("链上指标", "活跃地址、Gas、TVL、交易笔数等链上原生数据。", "它们能为数字资产研究提供不同于传统金融的数据视角。"),
                ],
                keywords=["区块链", "加密货币", "数字资产", "DeFi"],
            ),
            _section(
                "交易、执行与市场微观结构",
                "再好的研究也要通过真实市场微观结构才能变成可实现收益。",
                [
                    _entry("盘口", "买卖盘价格和挂单数量的实时结构。", "盘口结构会影响短线信号和成交概率。"),
                    _entry("买卖价差", "最优卖价与最优买价之间的差值。", "价差越大，进出场成本通常越高。"),
                    _entry("流动性", "资产在不显著冲击价格的前提下被交易的能力。", "很多策略回测赚钱，实盘却输在流动性不足。"),
                    _entry("滑点", "预期成交价与实际成交价之间的偏差。", "它会持续侵蚀策略收益。"),
                    _entry("市场冲击", "大单交易对市场价格造成的额外影响。", "容量评估和执行算法都离不开市场冲击分析。"),
                    _entry("订单有效期", "订单在撮合系统中的生效规则，例如 GTC、IOC、FOK。", "不同订单属性会改变成交结果和执行风险。"),
                ],
                keywords=["交易", "执行", "盘口", "微观结构"],
            ),
            _section(
                "量化研究、数据与因子",
                "量化研究的关键是把假设变成数据特征，再严谨验证。",
                [
                    _entry("数据清洗", "处理缺失值、异常值、重复值和时间对齐问题。", "脏数据会让任何模型都失真。"),
                    _entry("标签", "模型训练或评估时使用的目标变量，例如未来收益率。", "标签定义决定了模型到底在学什么。"),
                    _entry("因子", "可重复计算、可解释预期收益或风险的特征。", "因子是量化研究的基础语汇。"),
                    _entry("横截面研究 / 时序研究", "横截面比较同一时点不同资产，时序研究关注单一资产随时间变化。", "两类研究适用的信号和评价标准不同。"),
                    _entry("前视偏差", "把未来信息错误地用在当前决策中。", "它会让回测结果看起来异常完美。"),
                    _entry("过拟合", "模型过度适应历史噪声而非稳定规律。", "过拟合是策略上线后失效的常见原因。"),
                ],
                keywords=["量化", "数据", "因子", "回测"],
            ),
            _section(
                "风险管理与投资组合",
                "风险控制不是限制赚钱，而是确保赚钱的方法能活得久。",
                [
                    _entry("相关性", "两个资产收益共同变化的程度。", "看似分散的组合，可能因为高相关而风险集中。"),
                    _entry("杠杆", "用少量资本撬动更大敞口的机制。", "杠杆会同时放大收益和亏损。"),
                    _entry("VaR / CVaR", "分别衡量在给定置信水平下的潜在损失和尾部平均损失。", "它们是风险预算和压力讨论的常见语言。"),
                    _entry("再平衡", "把组合权重调整回目标配置。", "再平衡影响收益分布、换手和风险暴露。"),
                    _entry("压力测试", "用极端情景评估组合可能承受的损失。", "真实风险往往在平常样本里看不出来。"),
                    _entry("止损 / Kill Switch", "当价格、系统或账户状态异常时触发的风险保护机制。", "先控制尾部风险，才能谈长期复利。"),
                ],
                keywords=["风控", "组合", "仓位", "回撤"],
            ),
            _section(
                "银行体系与信用创造",
                "银行是货币传导、信用扩张和支付结算的核心节点。",
                [
                    _entry("商业银行", "吸收存款、发放贷款并提供支付结算服务的金融机构。", "它是信用创造的关键环节。"),
                    _entry("准备金", "银行在央行持有的存款或法定准备金。", "准备金制度影响流动性和银行信贷能力。"),
                    _entry("净息差", "银行生息资产收益率与付息负债成本之间的差额。", "净息差直接关系银行盈利能力。"),
                    _entry("资本充足率", "银行资本相对风险加权资产的比例。", "它是衡量银行稳健性的核心监管指标。"),
                    _entry("不良贷款", "借款人可能无法按时还款的贷款。", "不良率上升通常意味着信用周期承压。"),
                    _entry("存款保险", "对存款人在银行风险事件中的存款进行保障的制度。", "它关系到金融稳定与储户信心。"),
                ],
                keywords=["银行", "信用", "存款", "贷款"],
            ),
            _section(
                "保险与保障体系",
                "保险是风险转移机制，也是大型长期资金的重要来源。",
                [
                    _entry("保费", "投保人为获得保障而支付给保险公司的费用。", "它是保险经营现金流的源头。"),
                    _entry("承保", "保险公司评估风险并决定是否承接保险责任。", "定价是否合理取决于承保能力。"),
                    _entry("赔付率", "赔付金额相对于保费收入的比例。", "它帮助评估保险业务是否赚钱。"),
                    _entry("再保险", "保险公司将部分风险再转移给其他保险机构。", "再保险能降低单一事件对资产负债表的冲击。"),
                    _entry("偿付能力", "保险公司履行未来赔付责任的资本充足程度。", "这是监管和信用评估的重要指标。"),
                    _entry("资产负债匹配", "让保险资金投资期限和负债期限更加匹配。", "保险资金配置长期债券和高分红资产时必须考虑这一点。"),
                ],
                keywords=["保险", "保费", "赔付", "保障"],
            ),
            _section(
                "监管、合规与金融基础设施",
                "金融活动不只比收益，也比规则、流程和系统稳健性。",
                [
                    _entry("KYC / AML", "了解客户与反洗钱流程。", "开户、入金和机构合规都绕不开这套要求。"),
                    _entry("中央对手方", "在交易双方中间承担履约保证职责的清算机构。", "它降低了场内衍生品交易的对手风险。"),
                    _entry("托管", "由专业机构保管资产并监督交收。", "基金、券商和机构交易都依赖托管链路。"),
                    _entry("审计追踪", "保留关键操作、参数变更和审批记录。", "量化平台必须能解释谁在何时做了什么。"),
                    _entry("市场操纵", "通过虚假申报、拉抬打压等方式扭曲市场价格。", "策略设计必须避开合规红线。"),
                    _entry("数据治理", "围绕数据口径、权限、血缘和质量的管理体系。", "没有数据治理，量化平台很难长期稳定运行。"),
                ],
                keywords=["监管", "合规", "清算", "基础设施"],
            ),
            _section(
                "行为金融与市场情绪",
                "市场不只有理性定价，也有情绪、偏见和叙事循环。",
                [
                    _entry("损失厌恶", "人们对亏损的痛感通常大于同等收益带来的快乐。", "它解释了很多追涨杀跌和过早止盈行为。"),
                    _entry("锚定效应", "决策过度依赖某个初始参考点。", "投资者常被买入价、整数关口或旧高点锚定。"),
                    _entry("羊群效应", "在不确定环境中跟随大众行动。", "热门赛道和概念股泡沫常伴随羊群行为。"),
                    _entry("市场情绪", "投资者对未来的整体乐观或悲观程度。", "情绪因子常能解释短期波动和超额换手。"),
                    _entry("叙事交易", "市场围绕某个故事或主题快速定价。", "理解叙事有助于识别主题轮动和泡沫风险。"),
                    _entry("流动性挤兑", "市场恐慌时流动性突然消失、卖盘集中涌出。", "极端行情里，流动性风险常常大于估值风险。"),
                ],
                keywords=["行为金融", "情绪", "心理", "叙事"],
            ),
        ]

    def _build_study_plan(self) -> list[dict[str, Any]]:
        """Build the staged study plan."""

        return [
            {
                "stage_id": "stage_1",
                "title": "第 1 阶段：建立宏观与货币框架",
                "duration": "2-3 天",
                "goal": "先看懂经济周期、通胀、利率和汇率，不再把市场涨跌只归因于消息面。",
                "deliverables": ["能说清楚 GDP、通胀、政策利率和收益率曲线的基本作用", "知道宏观环境为什么会影响股票和债券估值"],
                "lessons": ["lesson_macro_cycle", "lesson_money_rates_fx"],
            },
            {
                "stage_id": "stage_2",
                "title": "第 2 阶段：认识主要市场与资产",
                "duration": "3-4 天",
                "goal": "建立股票、债券、期货、期权和数字资产的统一市场地图。",
                "deliverables": ["能区分一级市场和二级市场", "能解释股票、债券、期货、期权的核心差异"],
                "lessons": ["lesson_market_structure", "lesson_stocks_industries", "lesson_fixed_income_credit", "lesson_derivatives"],
            },
            {
                "stage_id": "stage_3",
                "title": "第 3 阶段：读懂公司、财报和估值",
                "duration": "3 天",
                "goal": "把公司分析从故事和概念，落到财务报表、现金流和估值框架。",
                "deliverables": ["能看懂三张基础财务报表", "知道 ROE、PE、PB、DCF 各自适合什么场景"],
                "lessons": ["lesson_financial_statements_valuation"],
            },
            {
                "stage_id": "stage_4",
                "title": "第 4 阶段：理解交易执行与数字市场",
                "duration": "2-3 天",
                "goal": "知道订单、盘口、滑点、区块链与链上数据为什么会影响真实结果。",
                "deliverables": ["能解释流动性、价差、部分成交", "能说明区块链、稳定币、DeFi 的基本关系"],
                "lessons": ["lesson_blockchain_digital_assets", "lesson_execution_microstructure"],
            },
            {
                "stage_id": "stage_5",
                "title": "第 5 阶段：进入量化研究主线",
                "duration": "3-4 天",
                "goal": "理解数据、因子、信号、回测和偏差控制的基本方法。",
                "deliverables": ["能说清楚因子和信号的区别", "能识别前视偏差、过拟合和数据污染"],
                "lessons": ["lesson_quant_data_process", "lesson_factor_backtest_bias"],
            },
            {
                "stage_id": "stage_6",
                "title": "第 6 阶段：补齐风控、机构与合规视角",
                "duration": "3 天",
                "goal": "把投资组合、银行、保险和监管放回同一张金融系统图中。",
                "deliverables": ["能解释组合风险和单笔风险的区别", "知道银行、保险和清算体系在金融系统中的作用"],
                "lessons": ["lesson_risk_portfolio", "lesson_banking_insurance_regulation"],
            },
        ]

    def _build_lessons(self) -> list[dict[str, Any]]:
        """Build the lesson catalog."""

        return [
            {
                "lesson_id": "lesson_macro_cycle",
                "title": "课程 1：宏观经济、通胀与财经周期",
                "duration": "30 分钟",
                "level": "Beginner",
                "goals": ["理解经济周期、通胀和就业如何影响风险资产", "知道财经新闻里的主要宏观指标在说什么"],
                "sections": [
                    {
                        "heading": "宏观不是遥远背景，而是估值底色",
                        "body": "经济增长、通胀和失业会同时影响企业盈利、居民消费、资金成本和风险偏好。",
                        "bullets": ["增长决定盈利弹性", "通胀决定利率压力", "就业决定消费和政策空间"],
                    },
                    {
                        "heading": "周期里要分清景气和拐点",
                        "body": "市场交易的经常不是当下数据本身，而是数据相对于预期的变化方向。",
                        "bullets": ["复苏早期更关注修复速度", "过热阶段更关注政策收紧", "衰退阶段更关注盈利和违约压力"],
                    },
                ],
                "practice": {
                    "prompt": "任选一条宏观新闻，用自己的话解释它更偏向增长、通胀还是政策信号。",
                    "checklist": ["有没有指出新闻对应的宏观变量", "有没有说明它可能影响哪些资产"],
                },
            },
            {
                "lesson_id": "lesson_money_rates_fx",
                "title": "课程 2：货币、利率、汇率与流动性",
                "duration": "30 分钟",
                "level": "Beginner",
                "goals": ["理解资金价格如何传导到资产定价", "知道货币宽松和紧缩为什么会改变市场风格"],
                "sections": [
                    {
                        "heading": "利率是资产定价的地心引力",
                        "body": "当无风险利率抬升时，远期现金流折现更厉害，成长资产通常更敏感。",
                        "bullets": ["利率影响估值倍数", "利率影响融资成本", "利率影响资金在股债现金之间的选择"],
                    },
                    {
                        "heading": "汇率连接国内市场和全球资本流动",
                        "body": "汇率不仅影响出口、进口和通胀，也会影响外资流向和跨市场估值比较。",
                        "bullets": ["强势货币有利于降低输入型通胀", "弱势货币可能改善出口竞争力", "跨境投资必须考虑汇率敞口"],
                    },
                ],
                "practice": {
                    "prompt": "试着解释一次“降息预期上升”为什么会同时影响股票、债券和汇率。",
                    "checklist": ["有没有提到资金成本", "有没有提到估值或资本流动"],
                },
            },
            {
                "lesson_id": "lesson_market_structure",
                "title": "课程 3：市场结构、交易场所与资产地图",
                "duration": "28 分钟",
                "level": "Beginner",
                "goals": ["建立一级市场、二级市场、交易所、券商和清算体系的整体认知", "知道不同资产类别为什么有不同交易制度"],
                "sections": [
                    {
                        "heading": "市场是一套制度，不只是价格曲线",
                        "body": "交易制度、涨跌停、盘前盘后、交收周期、保证金和申报规则都会影响可交易性。",
                        "bullets": ["股票强调公司与信息披露", "期货强调合约与保证金", "数字资产强调 24/7 与托管安全"],
                    },
                    {
                        "heading": "交易完成后还有清算和结算",
                        "body": "成交只是开始，真正的资产和资金交收需要清算、托管和账务系统协同完成。",
                        "bullets": ["结算周期影响资金占用", "清算机构降低对手风险", "系统设计必须考虑到账务对账"],
                    },
                ],
                "practice": {
                    "prompt": "分别用一句话描述交易所、券商和清算机构的角色。",
                    "checklist": ["有没有区分撮合、通道和交收", "有没有提到规则或风控"],
                },
            },
            {
                "lesson_id": "lesson_stocks_industries",
                "title": "课程 4：股票、行业与公司基本面",
                "duration": "32 分钟",
                "level": "Beginner",
                "goals": ["知道股票研究为什么离不开行业视角", "理解护城河、成长、分红和公司行为的含义"],
                "sections": [
                    {
                        "heading": "股票先是企业，再是代码",
                        "body": "好的股票研究不是盯着 K 线猜涨跌，而是理解企业商业模式、竞争位置和盈利驱动。",
                        "bullets": ["行业决定景气度", "公司决定执行力和护城河", "估值决定市场愿意付多高价格"],
                    },
                    {
                        "heading": "公司行为会改写价格序列",
                        "body": "分红、回购、送转和停复牌都会改变价格和流通股本，不处理好会直接污染回测。",
                        "bullets": ["分红影响股东总回报", "回购影响每股指标", "停复牌会影响流动性与估值连续性"],
                    },
                ],
                "practice": {
                    "prompt": "挑一只熟悉的股票，分别写出它所在行业、核心竞争力和最大的经营风险。",
                    "checklist": ["有没有写行业", "有没有写竞争优势", "有没有写风险来源"],
                },
            },
            {
                "lesson_id": "lesson_financial_statements_valuation",
                "title": "课程 5：财务报表、现金流与估值",
                "duration": "35 分钟",
                "level": "Beginner",
                "goals": ["能分清利润、现金流和资产负债结构", "知道常见估值指标分别适合什么场景"],
                "sections": [
                    {
                        "heading": "三张表要连起来看",
                        "body": "收入和利润决定表面表现，资产负债决定稳健程度，现金流决定真实成色。",
                        "bullets": ["利润高不一定现金流好", "负债高不一定坏，但要看成本和期限", "经营现金流更接近业务真实质量"],
                    },
                    {
                        "heading": "估值的核心是价格和价值之间的关系",
                        "body": "PE 适合盈利稳定公司，PB 常看金融和重资产，DCF 更强调长期自由现金流。",
                        "bullets": ["高增长不代表可以忽略估值", "低估值不代表一定便宜", "估值要结合行业和周期看"],
                    },
                ],
                "practice": {
                    "prompt": "选一家公司，尝试说出你更想先看利润表、现金流量表还是资产负债表，以及原因。",
                    "checklist": ["有没有给出选择理由", "有没有提到业务模式或风险"],
                },
            },
            {
                "lesson_id": "lesson_fixed_income_credit",
                "title": "课程 6：债券、利率与信用风险",
                "duration": "28 分钟",
                "level": "Beginner",
                "goals": ["知道债券收益的来源和利率风险的传导方式", "理解久期和信用利差的基础含义"],
                "sections": [
                    {
                        "heading": "债券不是只拿票息",
                        "body": "债券价格也会因为市场利率变化而波动，所以固定收益并不等于没有波动。",
                        "bullets": ["票息是现金流", "价格波动来自利率变化", "信用风险会带来额外利差"],
                    },
                    {
                        "heading": "利率风险和信用风险是两件事",
                        "body": "无风险利率变化影响所有折现资产，信用风险更多体现发行人本身的偿债能力。",
                        "bullets": ["久期衡量利率敏感度", "信用利差反映违约担忧", "流动性差会放大利差波动"],
                    },
                ],
                "practice": {
                    "prompt": "试着解释为什么“降息”通常更利好久期更长的债券。",
                    "checklist": ["有没有提到价格对利率的敏感度", "有没有提到久期"],
                },
            },
            {
                "lesson_id": "lesson_derivatives",
                "title": "课程 7：期货、期权与杠杆交易",
                "duration": "35 分钟",
                "level": "Intermediate",
                "goals": ["理解保证金、基差和展期的基本逻辑", "知道期权为什么是非线性工具"],
                "sections": [
                    {
                        "heading": "期货交易的是标准化合约",
                        "body": "期货引入了杠杆和到期日，所以除了方向判断，还必须管理保证金和展期。",
                        "bullets": ["保证金不足会被强平", "临近交割要注意移仓", "基差会影响套保和套利效果"],
                    },
                    {
                        "heading": "期权的风险不是线性的",
                        "body": "期权价格不仅看方向，也看波动率、时间价值和标的敏感度。",
                        "bullets": ["Delta 看方向敞口", "Theta 看时间损耗", "Vega 看波动率变化影响"],
                    },
                ],
                "practice": {
                    "prompt": "用自己的话解释：为什么同样看多，一个买期货，一个买看涨期权，风险结构并不一样？",
                    "checklist": ["有没有提到杠杆或保证金", "有没有提到期权时间价值或非线性"],
                },
            },
            {
                "lesson_id": "lesson_blockchain_digital_assets",
                "title": "课程 8：区块链、加密货币与链上金融",
                "duration": "32 分钟",
                "level": "Beginner",
                "goals": ["理解区块链、稳定币、DeFi 和链上数据的基本关系", "知道数字资产市场与传统金融市场的关键差异"],
                "sections": [
                    {
                        "heading": "区块链先是账本和网络，再是价格故事",
                        "body": "要理解数字资产，先要知道网络如何达成共识、如何记录交易、如何执行合约。",
                        "bullets": ["共识机制决定安全与效率", "智能合约决定应用能力", "钱包和托管决定资产控制方式"],
                    },
                    {
                        "heading": "数字资产市场的数据维度更多",
                        "body": "除了价格和成交量，链上地址、Gas、TVL、稳定币流入流出等都能成为研究输入。",
                        "bullets": ["链上数据更接近原生行为", "24/7 交易意味着风险不会休市", "托管与合约漏洞是传统市场少见风险"],
                    },
                ],
                "practice": {
                    "prompt": "列出数字资产市场相对传统股票市场最不同的三个点。",
                    "checklist": ["有没有提到 24/7", "有没有提到链上数据或托管", "有没有提到合约或稳定币"],
                },
            },
            {
                "lesson_id": "lesson_execution_microstructure",
                "title": "课程 9：交易执行、盘口与微观结构",
                "duration": "30 分钟",
                "level": "Intermediate",
                "goals": ["知道为什么订单状态会显著影响实际收益", "理解流动性、价差和市场冲击的现实意义"],
                "sections": [
                    {
                        "heading": "执行不是研究的附属品，而是收益的过滤器",
                        "body": "研究给出方向，执行决定你到底能以什么价格和什么成本实现它。",
                        "bullets": ["市价单强调确定成交", "限价单强调价格控制", "部分成交意味着真实流动性约束"],
                    },
                    {
                        "heading": "微观结构决定短期成本",
                        "body": "盘口深度、点差、冲击成本和订单排队位置都会影响实盘体验。",
                        "bullets": ["小盘股更容易受冲击", "高换手策略更怕点差和手续费", "大单执行通常需要算法拆单"],
                    },
                ],
                "practice": {
                    "prompt": "比较一次市价单和限价单的优劣，并说明你会在什么情况下选它们。",
                    "checklist": ["有没有提到成交概率", "有没有提到价格可控性"],
                },
            },
            {
                "lesson_id": "lesson_quant_data_process",
                "title": "课程 10：量化研究流程、数据与特征工程",
                "duration": "32 分钟",
                "level": "Beginner",
                "goals": ["掌握数据 -> 因子 -> 信号 -> 回测 -> 模拟交易的主流程", "知道数据清洗和标签定义为何重要"],
                "sections": [
                    {
                        "heading": "量化不是神秘模型，是严谨流程",
                        "body": "真正的量化研究先解决数据质量，再讨论模型复杂度。",
                        "bullets": ["数据口径错了，后面全错", "标签定义决定研究目标", "特征工程要能被解释和复现"],
                    },
                    {
                        "heading": "因子和模型都是对市场假设的编码",
                        "body": "动量、估值、质量、情绪等因子本质上是在表达“市场为什么会涨跌”的假设。",
                        "bullets": ["因子负责表达规律", "信号负责转成决策", "回测负责看历史上是否站得住"],
                    },
                ],
                "practice": {
                    "prompt": "写出一条最简单的量化研究流水线，每一步只用一句话。",
                    "checklist": ["是否包含数据准备", "是否包含因子或信号", "是否包含回测和模拟交易"],
                },
            },
            {
                "lesson_id": "lesson_factor_backtest_bias",
                "title": "课程 11：因子、回测与常见偏差",
                "duration": "35 分钟",
                "level": "Intermediate",
                "goals": ["知道前视偏差、数据泄漏和过拟合的主要表现", "理解为什么稳健性比漂亮曲线更重要"],
                "sections": [
                    {
                        "heading": "最危险的错误通常发生在你没注意的地方",
                        "body": "未来数据泄漏、样本内过度调参、幸存者偏差和数据对齐错误，都会让回测结果失真。",
                        "bullets": ["前视偏差让收益虚高", "过拟合让上线失效", "幸存者偏差会忽略退市或失败样本"],
                    },
                    {
                        "heading": "稳健回测比高收益更重要",
                        "body": "好的回测不是年化越高越好，而是逻辑清晰、风险可解释、换手和成本合理。",
                        "bullets": ["要看收益也要看回撤", "要看参数敏感性", "要看样本内外和不同市场环境"],
                    },
                ],
                "practice": {
                    "prompt": "说出一个你认为会让回测虚高的错误，并说明它为什么危险。",
                    "checklist": ["有没有说清错误机制", "有没有说明它如何误导决策"],
                },
            },
            {
                "lesson_id": "lesson_risk_portfolio",
                "title": "课程 12：风险管理、组合构建与资金纪律",
                "duration": "33 分钟",
                "level": "Beginner",
                "goals": ["理解组合风险和单笔风险的差异", "知道仓位、分散、再平衡和压力测试的基本作用"],
                "sections": [
                    {
                        "heading": "组合不是把几个看好的标的拼在一起",
                        "body": "组合管理的核心是控制相关性、集中度、杠杆和尾部风险，而不是只追求更多持仓数量。",
                        "bullets": ["多只同主题股票不等于分散", "相关性高时回撤会同步放大", "再平衡帮助组合回到目标风险"],
                    },
                    {
                        "heading": "资金纪律决定你能不能活过坏行情",
                        "body": "不设仓位上限、不做压力测试、不准备 kill switch，往往在极端行情中代价最大。",
                        "bullets": ["仓位决定波动承受力", "压力测试决定极端场景预案", "Kill switch 是系统性保护"],
                    },
                ],
                "practice": {
                    "prompt": "如果你只能配 5 个仓位，会如何设单仓上限和行业上限？",
                    "checklist": ["有没有提到单仓权重", "有没有提到行业或主题分散"],
                },
            },
            {
                "lesson_id": "lesson_banking_insurance_regulation",
                "title": "课程 13：银行、保险、监管与金融系统稳定",
                "duration": "32 分钟",
                "level": "Beginner",
                "goals": ["把银行、保险、监管和清算基础设施放到同一张系统图里", "理解金融机构为何会影响流动性与市场风险偏好"],
                "sections": [
                    {
                        "heading": "银行和保险是金融系统的重要缓冲层",
                        "body": "银行提供信用和支付，保险承接风险并形成长期资金，两者都深度影响资产价格和流动性。",
                        "bullets": ["银行决定信用扩张速度", "保险影响长期资金配置", "机构稳健性影响市场信心"],
                    },
                    {
                        "heading": "监管与基础设施决定系统能否稳住",
                        "body": "KYC、AML、清算、托管、审计追踪和风险资本要求，都是金融体系稳定运行的底层装置。",
                        "bullets": ["监管不是阻碍，而是边界", "托管和清算保护资产安全", "量化平台也需要合规和审计能力"],
                    },
                ],
                "practice": {
                    "prompt": "用自己的话解释：为什么一个只做股票研究的人，也应该懂银行、保险和监管？",
                    "checklist": ["有没有提到流动性或信用", "有没有提到系统稳定或资金来源"],
                },
            },
        ]

    def _build_quiz(self) -> dict[str, Any]:
        """Build the knowledge check quiz."""

        return {
            "title": "金融与量化基础综合测验",
            "pass_score": 80,
            "questions": [
                {
                    "question_id": "q1",
                    "lesson_id": "lesson_macro_cycle",
                    "prompt": "下面哪项最直接描述“整体物价水平持续上升”？",
                    "options": [
                        {"option_id": "a", "text": "通货膨胀"},
                        {"option_id": "b", "text": "失业率"},
                        {"option_id": "c", "text": "股息率"},
                        {"option_id": "d", "text": "换手率"},
                    ],
                    "correct_option_id": "a",
                    "explanation": "通货膨胀描述的是整体价格水平上升，会影响货币购买力与利率路径。",
                },
                {
                    "question_id": "q2",
                    "lesson_id": "lesson_money_rates_fx",
                    "prompt": "哪一个变量通常最直接影响资产估值中的折现率？",
                    "options": [
                        {"option_id": "a", "text": "行业分类"},
                        {"option_id": "b", "text": "无风险利率"},
                        {"option_id": "c", "text": "董事会人数"},
                        {"option_id": "d", "text": "仓位上限"},
                    ],
                    "correct_option_id": "b",
                    "explanation": "无风险利率是大多数资产定价模型的基础折现率输入之一。",
                },
                {
                    "question_id": "q3",
                    "lesson_id": "lesson_market_structure",
                    "prompt": "成交完成后，负责资金和证券最终交收的环节叫做什么？",
                    "options": [
                        {"option_id": "a", "text": "结算"},
                        {"option_id": "b", "text": "做市"},
                        {"option_id": "c", "text": "选股"},
                        {"option_id": "d", "text": "复盘"},
                    ],
                    "correct_option_id": "a",
                    "explanation": "结算负责最终交收，决定资产和资金何时真正到位。",
                },
                {
                    "question_id": "q4",
                    "lesson_id": "lesson_stocks_industries",
                    "prompt": "研究股票时，下面哪项最能体现企业长期竞争优势？",
                    "options": [
                        {"option_id": "a", "text": "护城河"},
                        {"option_id": "b", "text": "随机波动"},
                        {"option_id": "c", "text": "盘口挂单"},
                        {"option_id": "d", "text": "撮合速度"},
                    ],
                    "correct_option_id": "a",
                    "explanation": "护城河描述企业维持竞争优势和超额回报的能力。",
                },
                {
                    "question_id": "q5",
                    "lesson_id": "lesson_financial_statements_valuation",
                    "prompt": "哪张报表最适合观察企业经营活动是否真正回笼现金？",
                    "options": [
                        {"option_id": "a", "text": "利润表"},
                        {"option_id": "b", "text": "资产负债表"},
                        {"option_id": "c", "text": "现金流量表"},
                        {"option_id": "d", "text": "股东名册"},
                    ],
                    "correct_option_id": "c",
                    "explanation": "现金流量表更直接反映企业经营现金流入流出情况。",
                },
                {
                    "question_id": "q6",
                    "lesson_id": "lesson_fixed_income_credit",
                    "prompt": "债券价格对利率变化的敏感度，常用哪个指标衡量？",
                    "options": [
                        {"option_id": "a", "text": "分红率"},
                        {"option_id": "b", "text": "久期"},
                        {"option_id": "c", "text": "市净率"},
                        {"option_id": "d", "text": "胜率"},
                    ],
                    "correct_option_id": "b",
                    "explanation": "久期越长，债券价格通常对利率变动越敏感。",
                },
                {
                    "question_id": "q7",
                    "lesson_id": "lesson_derivatives",
                    "prompt": "下面哪项最能体现期货交易的杠杆特征？",
                    "options": [
                        {"option_id": "a", "text": "保证金制度"},
                        {"option_id": "b", "text": "公司分红"},
                        {"option_id": "c", "text": "固定票息"},
                        {"option_id": "d", "text": "回购注销"},
                    ],
                    "correct_option_id": "a",
                    "explanation": "保证金制度让交易者用较少资金控制更大名义敞口。",
                },
                {
                    "question_id": "q8",
                    "lesson_id": "lesson_blockchain_digital_assets",
                    "prompt": "下面哪项最符合稳定币的定义？",
                    "options": [
                        {"option_id": "a", "text": "价格永远上涨的代币"},
                        {"option_id": "b", "text": "锚定法币或其他资产价值的数字代币"},
                        {"option_id": "c", "text": "只能在交易所内部流通的积分"},
                        {"option_id": "d", "text": "没有链上记录的代币"},
                    ],
                    "correct_option_id": "b",
                    "explanation": "稳定币的目标是保持相对稳定的价值锚定，常用于结算与流动性管理。",
                },
                {
                    "question_id": "q9",
                    "lesson_id": "lesson_execution_microstructure",
                    "prompt": "下面哪项最直接描述预期成交价与实际成交价的差异？",
                    "options": [
                        {"option_id": "a", "text": "滑点"},
                        {"option_id": "b", "text": "净息差"},
                        {"option_id": "c", "text": "夏普比率"},
                        {"option_id": "d", "text": "资本充足率"},
                    ],
                    "correct_option_id": "a",
                    "explanation": "滑点反映执行过程中由于流动性和市场波动产生的额外成本。",
                },
                {
                    "question_id": "q10",
                    "lesson_id": "lesson_quant_data_process",
                    "prompt": "量化研究从想法走向交易前，哪条路径更合理？",
                    "options": [
                        {"option_id": "a", "text": "直觉 -> 满仓 -> 复盘"},
                        {"option_id": "b", "text": "数据 -> 因子 / 特征 -> 信号 -> 回测 -> 模拟交易"},
                        {"option_id": "c", "text": "新闻 -> 跟单 -> 加杠杆"},
                        {"option_id": "d", "text": "只看排行榜 -> 直接实盘"},
                    ],
                    "correct_option_id": "b",
                    "explanation": "量化流程的关键是把假设编码成规则，并通过回测和模拟交易验证。",
                },
                {
                    "question_id": "q11",
                    "lesson_id": "lesson_factor_backtest_bias",
                    "prompt": "哪种情况最可能构成前视偏差？",
                    "options": [
                        {"option_id": "a", "text": "按真实披露时间使用财务数据"},
                        {"option_id": "b", "text": "回测时用到了未来才会公布的财报数据"},
                        {"option_id": "c", "text": "按时间顺序读取历史 K 线"},
                        {"option_id": "d", "text": "对多个股票使用相同因子定义"},
                    ],
                    "correct_option_id": "b",
                    "explanation": "未来数据泄漏会让历史结果被系统性高估。",
                },
                {
                    "question_id": "q12",
                    "lesson_id": "lesson_risk_portfolio",
                    "prompt": "为什么组合层风控比只看单笔交易更重要？",
                    "options": [
                        {"option_id": "a", "text": "因为组合天然不会亏损"},
                        {"option_id": "b", "text": "因为多个高相关仓位可能一起放大整体回撤"},
                        {"option_id": "c", "text": "因为只要持仓多就一定分散"},
                        {"option_id": "d", "text": "因为回撤和仓位没有关系"},
                    ],
                    "correct_option_id": "b",
                    "explanation": "组合风险取决于相关性、集中度和杠杆，不是只看单笔胜率。",
                },
                {
                    "question_id": "q13",
                    "lesson_id": "lesson_banking_insurance_regulation",
                    "prompt": "下面哪项最符合银行在金融系统中的核心作用？",
                    "options": [
                        {"option_id": "a", "text": "主要负责艺术品定价"},
                        {"option_id": "b", "text": "通过吸收存款和发放贷款参与信用创造"},
                        {"option_id": "c", "text": "只负责股票涨跌停设置"},
                        {"option_id": "d", "text": "只负责区块链记账"},
                    ],
                    "correct_option_id": "b",
                    "explanation": "商业银行通过存贷款和支付结算参与信用创造和资金传导。",
                },
            ],
        }
