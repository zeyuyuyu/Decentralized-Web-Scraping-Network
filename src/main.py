import os
import openai

# Set up OpenAI API key
openai.api_key = os.environ.get('OPENAI_API_KEY')

def generate_code(prompt):
    """
    Generate code based on a natural language prompt using GPT-3.
    
    Args:
        prompt (str): The natural language prompt describing the desired code.
    
    Returns:
        str: The generated code.
    """
    response = openai.Completion.create(
        engine=\"code-davinci-002\