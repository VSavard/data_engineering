import pandas as pd

def excel_to_df(path: str, sheet: str) -> pd.DataFrame:
    return pd.read_excel(io=path, sheet_name=sheet).astype(str)
