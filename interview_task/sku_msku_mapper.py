import pandas as pd
import numpy as np
import os



class SKUMapper:
    def __init__(self):
        self.mappings = {}
        self.logs = []

    def log(self, message):
        """Log the mapping process."""
        self.logs.append(message)

    def load_master_mapping_for_multimarket(self, df_sheet_msku_sku, df_sheet_combos_sku, INPUT_DIRECTORY):
        # Store the original dataframes
        self.sheet_msku_sku = df_sheet_msku_sku
        self.sheet_combos_sku = df_sheet_combos_sku

        try:
            list_of_df = []
            for file in os.scandir(INPUT_DIRECTORY):
                self.log(file.name)
                if file and "." not in file.name:
                    for csv_file in os.scandir(file.path):
                        if csv_file:
                            df = pd.read_csv(csv_file.path)
                            if "AMAZON" in file.name:
                                temp_df = df.filter(["Date", "MSKU", "Fulfillment Center"])
                                temp_df["Panel"] = ["Amazon"] * len(temp_df)
                            elif "GL" in file.name:
                                temp_df = df.filter(["Ordered On", "SKU", "State"]).rename(columns={"Ordered On":"Date"}) # rename works here only on main df
                                temp_df["Panel"] = ["GIANT LEAP FLIPKART"] * len(temp_df)
                            elif "CSTE FK" in file.name:
                                temp_df = df.filter(["Ordered On", "SKU", "State"]).rename(columns={"Ordered On":"Date"})
                                temp_df["Panel"] = ["CSTE FLIPKART"] * len(temp_df)
                            elif "RUDRAV MEESHO" in file.name:
                                temp_df = df.filter(["Order Date", "SKU", "Customer State"]).rename(columns={'Order Date':"Date"})
                                temp_df.rename(columns={"Customer State":"State"}, inplace=True)
                                temp_df["Panel"] = ["RUDRAV MEESHO"] * len(temp_df)    
                            list_of_df.append(temp_df)

            all_df = pd.concat(list_of_df)
            all_df_part1 = all_df.dropna(subset=["State"])

            all_df.loc[~all_df["Fulfillment Center"].notna(), "Fulfillment Center"] = all_df_part1["State"].values.tolist()
            all_df_new = all_df.drop(columns=["State"])

            all_df_new.to_csv(f"{INPUT_DIRECTORY}/all_data.csv", index=False)
            df_all = pd.read_csv(f"{INPUT_DIRECTORY}/all_data.csv")
            df_all['Date'] = pd.to_datetime(df_all['Date'], errors='coerce', infer_datetime_format=True) # for NaT
            df_all["Date"] = df_all["Date"].astype("str").replace("NaT", np.nan)

            if self.mappings is not None:
                self.load_sku_msku_mapping(df_all, df_sheet_msku_sku)
                self.load_combo_mapping(df_all, df_sheet_combos_sku)
                df_merge1 = self.mappings['sku_msku1']
                df_merge2 = self.mappings["sku_msku2"]
                df_merge_combo = self.mappings['combo']

                df_msku_sku_final = pd.concat([df_merge1, df_merge2])
                df_merge_combo.drop(["MSKU", "Date"], axis=1, inplace=True)
                column_list = df_merge_combo.columns.to_list()
                # List of SKU columns to be split
                sku_columns = column_list[3:len(column_list)-1]

                dfs = []
                for col in sku_columns:
                    temp_df = df_merge_combo[['Warehouse', 'Panel', 'SKU', col, 'Status']].copy()
                    temp_df = temp_df.rename(columns={col: 'msku'})
                    dfs.append(temp_df)

                result_df = pd.concat(dfs)
                result_df = result_df[result_df['msku'].notna()].reset_index(drop=True).rename(columns={"SKU":"sku"})
                final_df = pd.concat([df_msku_sku_final, result_df])
                msku_count_for_sku = final_df.groupby("sku")["msku"].nunique().reset_index()
                msku_count_for_sku = msku_count_for_sku.rename(columns={"msku":"Quantity"})
                final_df1 = pd.merge(final_df, msku_count_for_sku, on=["sku"], how="left")

                self.mappings["final_data_df"] = final_df1
            self.log(f"All market data files mapped to final dataframe succesfully.")
        except Exception as e:
            self.log(f"Error loading master mapping: {e}")

    def load_sku_msku_mapping(self, df_all, df_sheet_msku_sku):
        try:
            # Merge the dataframes on 'sku' column and handle missing values
            df_sheet_msku_sku1 = df_sheet_msku_sku.filter(["sku", "msku", "Status", "panels"])
            matching_values = df_all["SKU"].isin(df_sheet_msku_sku1["sku"])
            matching_df_all = df_all[matching_values].rename(columns={"MSKU":"msku", "SKU":"sku", "Fulfillment Center":"Warehouse"}).reset_index(drop=True)
            matching_values_sheet1 = df_sheet_msku_sku1["sku"].isin(matching_df_all["sku"])
            matching_df_sheet1 = df_sheet_msku_sku1[matching_values_sheet1].rename(columns={"panels":"Panel"}).reset_index(drop=True)
            matching_df_sheet2 = matching_df_sheet1.replace({'Panel':{"CSTE FK":"CSTE FLIPKART", "GL FK":"GIANT LEAP FLIPKART", "CSTE AMAZON":"Amazon", "Rudrav Meesho":"RUDRAV MEESHO"}})
            df_merge = pd.merge(matching_df_all, matching_df_sheet2, on='sku', how='outer', suffixes=('_df1', '_df2'))
            df_merge['msku'] = df_merge['msku_df1'].combine_first(df_merge['msku_df2'])
            df_merge['Panel'] = df_merge['Panel_df1'].combine_first(df_merge['Panel_df2'])
            df_merge.drop(['msku_df1', 'msku_df2', 'Panel_df1', 'Panel_df2'], axis=1, inplace=True)
            df_merge1 = df_merge.drop_duplicates().reset_index(drop=True)

            df_all_msku = df_all[df_all['MSKU'].notna()].rename(columns={'Fulfillment Center':"Warehouse"}).reset_index(drop=True)
            matching_msku_values = df_all_msku["MSKU"].isin(df_sheet_msku_sku1["msku"])
            df_matching_msku = df_all_msku[matching_msku_values].rename(columns={"MSKU":"msku", "SKU":"sku"}).reset_index(drop=True)
            matching_msku_sheet_values = df_sheet_msku_sku1["msku"].isin(df_all_msku["MSKU"])
            matching_msku_sheet = df_sheet_msku_sku1[matching_msku_sheet_values].replace({'panels':{"CSTE FK":"CSTE FLIPKART", "GL FK":"GIANT LEAP FLIPKART", "CSTE AMAZON":"Amazon", "Rudrav Meesho":"RUDRAV MEESHO"}}).rename(columns={"panels":"Panel"}).reset_index(drop=True)
            df_merge_msku = pd.merge(df_matching_msku, matching_msku_sheet, on=["msku"], how="outer", suffixes=("_df1", "_df2"))
            df_merge_msku['sku'] = df_merge_msku['sku_df1'].combine_first(df_merge_msku['sku_df2'])
            df_merge_msku['Panel'] = df_merge_msku['Panel_df1'].combine_first(df_merge_msku['Panel_df2'])
            df_merge_msku.drop(['sku_df1', 'sku_df2', 'Panel_df1', 'Panel_df2'], axis=1, inplace=True)
            df_merge2 = df_merge_msku.drop_duplicates().reset_index(drop=True)

            self.mappings['sku_msku1'] = df_merge1
            self.mappings["sku_msku2"] = df_merge2
            self.log("SKU to MSKU mapping loaded successfully.")
        except Exception as e:
            self.log(f"Error loading SKU to MSKU mapping: {e}")

    def load_combo_mapping(self, df_all, df_sheet_combos_sku):
        try:
            # Process combo product mapping
            df_sheet_combos_sku = df_sheet_combos_sku.rename(columns={"Combo ":"Combo"})
            matching_values_combo = df_all["SKU"].isin(df_sheet_combos_sku["Combo"])
            matching_df_combo = df_all[matching_values_combo].reset_index(drop=True)
            matching_df_sheet_combo_values = df_sheet_combos_sku["Combo"].isin(df_all["SKU"])
            matching_df_sheet_combo = df_sheet_combos_sku[matching_df_sheet_combo_values].drop('Unnamed: 16', axis=1).reset_index(drop=True).rename(columns={"Combo":"SKU"})
            df_merge_combo = pd.merge(matching_df_combo, matching_df_sheet_combo, on=["SKU"], how="outer", suffixes=("_df1", "_df2"))
            df_merge_combo = df_merge_combo.rename(columns={"Fulfillment Center": "Warehouse"})

            self.mappings['combo'] = df_merge_combo
            self.log("Combo product mapping loaded successfully.")
        except Exception as e:
            self.log(f"Error loading combo product mapping: {e}")

    def get_mapping(self, name):
        return self.mappings.get(name)

    def get_logs(self):
        return self.logs



if __name__ == "__main__":

    INPUT_DIRECTORY = 'C:\\Users\\mishr\\OneDrive\\Documents\\SMcodes8\\webscraping_task\\interview_task'
    INPUT_FILE_NAME = "WMS-04-02.xlsx"
    SHEET_NAME = "Msku With Skus"
    SHEET_NAME2 = "Combos skus"

    df_sheet_msku_sku = pd.read_excel(f"{INPUT_DIRECTORY}/{INPUT_FILE_NAME}", sheet_name=SHEET_NAME)
    df_sheet_combos_sku = pd.read_excel(f"{INPUT_DIRECTORY}/{INPUT_FILE_NAME}", sheet_name=SHEET_NAME2)

    # Instantiate the SKUMapper class
    sku_mapper = SKUMapper()

    # Load master mapping
    sku_mapper.load_master_mapping_for_multimarket(df_sheet_msku_sku=df_sheet_msku_sku, df_sheet_combos_sku=df_sheet_combos_sku, INPUT_DIRECTORY=INPUT_DIRECTORY)

    # Get the final mapping and save it to csv
    final_mapping = sku_mapper.get_mapping("final_data_df")
    final_mapping.to_csv(f"{INPUT_DIRECTORY}/final_outbound_data.csv", index=False)

    # Get the logs of the mapping process
    logs = sku_mapper.get_logs()
    print(logs)
