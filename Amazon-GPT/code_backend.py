from langchain_community.llms import Bedrock
from langchain.memory import ConversationBufferMemory
from langchain.chains import ConversationChain

def demo_chatbot():
    demo_llm = Bedrock(
        credentials_profile_name='default',
        model_id='meta.llama2-70b-chat-v1',
        model_kwargs={
            "temperature": 0.9,
            "top_p": 0.5,
            "max_gen_len": 512
        }
    )
    return demo_llm

def demo_memory():
    memory = ConversationBufferMemory(return_messages=True, max_token_limit=512)
    return memory

def demo_chain(input_text, memory):
    llm_chain_data = demo_chatbot()
    llm_conversation = ConversationChain(llm=llm_chain_data, memory=memory, verbose=True)
    chat_reply = llm_conversation.predict(input=input_text)
    return chat_reply

# Example usage:
# if _name_ == "_main_":
    memory = demo_memory()
    response = demo_chain('Hi, What is your name?', memory)
    print(response)