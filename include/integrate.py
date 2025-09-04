def integrateRecords(jobberman_data, myjobmag_data):
        import pandas as pd 

        combined_df = pd.concat([pd.DataFrame(jobberman_data), pd.DataFrame(myjobmag_data)], ignore_index=True)
        return combined_df.to_dict(orient="records")
    