import os
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI

load_dotenv()

MODEL_NAME = "gemini-3.1-flash-lite-preview"
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "YOUR_API_KEY_HERE")

PLACEHOLDER_VALUES = {"n.d.", "NULL", "unknown", "?", "//", "-", "null", "N/A",
                      "undefined", "ND", "", " ", "nan"}


def get_llm(temperature: float = 0) -> ChatGoogleGenerativeAI:
    return ChatGoogleGenerativeAI(
        model=MODEL_NAME,
        google_api_key=GOOGLE_API_KEY,
        temperature=temperature,
    )
