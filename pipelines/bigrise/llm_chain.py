from langchain.agents import initialize_agent, Tool, AgentType
from langchain.chat_models import ChatOpenAI

# 도구 정의
def summarize_news():
    ...
def analyze_bigrise():
    ...
def correlate_industries():
    ...
def write_report():
    ...

tools = [
    Tool(name="SummarizeNews", func=summarize_news, description="뉴스 요약"),
    Tool(name="AnalyzeBigRise", func=analyze_bigrise, description="산업/ETF 분석"),
    Tool(name="CorrelateIndustries", func=correlate_industries, description="산업간 상관관계 도출"),
    Tool(name="WriteReport", func=write_report, description="최종 리포트 작성"),
]

llm = ChatOpenAI(model="gpt-4o", temperature=0.3)

agent = initialize_agent(tools, llm, agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION, verbose=True)

agent.run("세 데이터셋을 분석해서 투자 리포트를 작성해줘.")
