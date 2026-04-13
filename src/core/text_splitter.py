"""
Text Splitter Module
--------------------
Implements a lightweight, word-based text splitter that chunks long
strings into overlapping segments using configurable chunk size and
overlap, avoiding heavier tokenization dependencies.
"""

from typing import List


class TokenTextSplitter:
    """
    Splits text into chunks based on whitespace-separated words.
    Based on logic from original notebooks.
    """

    def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 200):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    def split_text(self, text: str) -> List[str]:
        if not text:
            return []
        words = text.split()
        chunks = []
        if len(words) <= self.chunk_size:
            return [text]

        step = self.chunk_size - self.chunk_overlap
        if step <= 0:
            step = 1  # Avoid infinite loop

        for i in range(0, len(words), step):
            chunk_words = words[i : i + self.chunk_size]
            chunk = " ".join(chunk_words)
            chunks.append(chunk)

        return chunks
