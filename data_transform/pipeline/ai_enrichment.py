import pandas as pd
import time
from abc import ABC, abstractmethod


############################################################
# Base class
############################################################

class AIEnricher(ABC):

    def __init__(self, api_key, model=None, delay=0):
        self.api_key = api_key
        self.model = model
        self.delay = delay

    @abstractmethod
    def call_model(self, prompt):
        pass

    def enrich_dataframe(self, df, text_column, output_column):

        results = []

        for text in df[text_column].fillna(""):

            try:

                result = self.call_model(text)

            except Exception as e:

                print("API error:", e)
                result = None

            results.append(result)

            if self.delay:
                time.sleep(self.delay)

        df[output_column] = results

        return df


############################################################
# ChatGPT implementation
############################################################

class ChatGPTEnricher(AIEnricher):

    def __init__(self, api_key, model="gpt-4o-mini", delay=0):

        super().__init__(api_key, model, delay)

        from openai import OpenAI
        self.client = OpenAI(api_key=api_key)

    def call_model(self, prompt):

        response = self.client.chat.completions.create(

            model=self.model,

            messages=[

                {"role": "system", "content": "Extract structured rental listing features."},

                {"role": "user", "content": prompt}

            ]

        )

        return response.choices[0].message.content


############################################################
# Gemini implementation
############################################################

class GeminiEnricher(AIEnricher):

    def __init__(self, api_key, model="gemini-1.5-flash", delay=0):

        super().__init__(api_key, model, delay)

        import google.generativeai as genai

        genai.configure(api_key=api_key)

        self.model_instance = genai.GenerativeModel(model)

    def call_model(self, prompt):

        response = self.model_instance.generate_content(prompt)

        return response.text


############################################################
# Factory
############################################################

def create_enricher(provider, api_key, model=None, delay=0):

    provider = provider.lower()

    if provider == "chatgpt":

        return ChatGPTEnricher(api_key, model, delay)

    elif provider == "gemini":

        return GeminiEnricher(api_key, model, delay)

    else:

        raise ValueError("Provider must be 'chatgpt' or 'gemini'")