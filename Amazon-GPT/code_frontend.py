
import streamlit as st
import code_backend as demo

#Title_for_chatbot
st.title("Hello, This is MAS-GPT.")
st.markdown("<p style='font-size: small;'><em>~By Ashish, Sujit, Murali</em></p>", unsafe_allow_html=True)
if 'memory' not in st.session_state:
    st.session_state.memory = demo.demo_memory()

# UI changes
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

#Providing chat history

for message in st.session_state.chat_history:
    with st.chat_message(message["role"]):
        st.markdown(message["text"])

#Input
input_text = st.chat_input("Chat with MAS_GPT here!!!")
if input_text:

    with st.chat_message("user"):
        st.markdown(input_text)
    st.session_state.chat_history.append({"role":"user", "text":input_text})

    chat_response = demo.demo_chain(input_text = input_text, memory = st.session_state.memory)

    with st.chat_message("assistant"):
        st.markdown(chat_response)

    st.session_state.chat_history.append({"role":"assistant","text":chat_response})
