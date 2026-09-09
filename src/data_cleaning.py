from pyspark.sql import SparkSession
import os 
from  pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType
import pyspark.sql.functions as F 


def initialize_spark():
    return SparkSession.builder.appName('FinancialRiskPipeline').config("spark.driver.memory","2g").getOrCreate()

def get_loan_Schema():
    """Defines a Professional Schema matching the loan Risk dataset structure."""
    return StructType([
        StructField("ApplicationDate",StringType(),True),
        StructField("Age",IntegerType(),True),
        StructField("AnnualIncome",IntegerType(),True),
        StructField("CreditScore",IntegerType(),True),
        StructField("EmploymentStatus",StringType(),True),
        StructField("EducationLevel",StringType(),True),
        StructField("ExperienceYears",IntegerType(),True),
        StructField("ExistingDebt",IntegerType(),True),
        StructField("LoanDurationMonths",IntegerType(),True),
        StructField("MaritalStatus",StringType(),True),
        StructField("NumberOfDependents",IntegerType(),True),
        StructField("HomeOwnership",StringType(),True),
        StructField("LoanAmountRequested",IntegerType(),True),
        StructField("DebtToIncomeRatio",DoubleType(),True),
        StructField("AssetValue",IntegerType(),True),
        StructField("SavingsBalance",IntegerType(),True),
        StructField("CheckingBalance",IntegerType(),True),
        StructField("PastDefaultHistory", IntegerType(),True),
        StructField("LoanPurpose", StringType(),True),
        StructField("CoSignerAvailable", IntegerType(),True),
        StructField("OpenCreditLines", IntegerType(),True),
        StructField("TotalCreditLimits", IntegerType(),True),
        StructField("MonthlyDebtPayment", IntegerType(), True),
        StructField("UtilityBillsStatus", IntegerType(), True),
        StructField("NetWorth", IntegerType(), True),
        StructField("InvestmentsValue", IntegerType(), True),
        StructField("MonthlyExpenses", DoubleType(), True),
        StructField("CreditUtilizationRate", DoubleType(), True),
        StructField("DerogatoryMarks", IntegerType(), True),
        StructField("TotalAssetsValue", IntegerType(), True),
        StructField("RiskScore", DoubleType(), True),
        StructField("InterestRateRequested", DoubleType(), True),
        StructField("MonthlyInstallment", DoubleType(), True),
        StructField("EmploymentDurationMonths", DoubleType(), True),
        StructField("LoanApprovedStatus", IntegerType(), True),
        StructField("Placeholder_AgeGroup", DoubleType(), True)
    ])

def clean_and_Transfrom(df):
    
    df_clean=df.withColumn("ApplicationDate",F.to_date("ApplicationDate","yyyy-MM-dd"))

    df_clean = df_clean.na.fill("Unknown",["EmploymentStatus","EducationLevel","HomeOwnership","LoanPurpose"])

    df_clean= df_clean.na.fill(0)

    df_clean = df_clean.withColumn(
        "DebtToAssetRatio",
        F.round(F.col("ExistingDebt")/F.greatest(F.col("TotalAssetsValue"), F.lit(1)),4)

    )

    return df_clean


if __name__ == "__main__":
    spark = initialize_spark()

    raw_data_path = os.path.join("data","raw","Financial.csv")

    df=spark.read.csv(raw_data_path, header=True, schema=get_loan_Schema())

    cleaned_df = clean_and_Transfrom(df)


    print("\n Data Successfully Processed & Engineered!")     

    cleaned_df.select("ApplicationDate","Age","AnnualIncome","LoanAmountRequested","LoanApprovedStatus","DebtToAssetratio").show()


    #---Windows Local Workload for Portfolio---
    # we comment out the physical file write to bypass windows winutils.exe restrictions.
    # In a production cloud cluster (Databricks/GCP), this line would run natively.
    #processed_path = os.path.join("data","processed","cleaned_loan_data")
    #cleaned_df.write.mode("overwrite").parquet(processed_path)
    #print(f"Processed file saved securely in parquet format at: {processed_path}")

    print("Pipeline complete! ready for data aggregation step.")
    spark.stop()    