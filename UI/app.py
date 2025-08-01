import requests
import pandas as pd
import streamlit as st

st.title("Text-to-SQL Assistant ")
question = st.text_input("Question", value="What is the revenue per ton of cargo in 2023-24?")

if st.button("Generate Answer"):
    payload = {
        "question": question,
        "return_table": True,
    }
    r = requests.post("http://localhost:8000/ask", json=payload, timeout=120)
    data = r.json()

    st.write("**SQL**")
    st.code(data.get("sql") or "", language="sql")

    st.write("**Answer**")
    st.success(data.get("answer") or "")

    cols = data.get("columns")
    rows = data.get("rows")
    if cols and rows:
        df = pd.DataFrame(rows, columns=cols)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No table to display.")
