import great_expectations as ge
from great_expectations.dataset import SparkDFDataset

def validate_transactions(spark_df):
    ge_df = SparkDFDataset(spark_df)

    ge_df.expect_column_values_to_not_be_null("Trans_id")
    ge_df.expect_column_values_to_not_be_null("Clt_id")
    ge_df.expect_column_values_to_be_between("Trans_amount", min_value=0, max_value=100000)
    ge_df.expect_column_values_to_match_regex("Currency", "^[A-Z]{3}$")
    ge_df.expect_column_values_to_be_in_set("Trans_status", ["SUCCESS", "FAILED", "PENDING"])
    ge_df.expect_column_values_to_be_in_set("Trans_type", ["POS", "ATM", "ONLINE", "TRANSFER"])

    return ge_df.validate()

def validate_clients(spark_df):
    ge_df = SparkDFDataset(spark_df)

    ge_df.expect_column_values_to_not_be_null("Clt_id")
    ge_df.expect_column_values_to_not_be_null("Card_id")
    ge_df.expect_column_values_to_match_regex("Clt_phoneNumber", r"^\+?[0-9]{7,15}$")
    ge_df.expect_column_values_to_be_between("Clt_age", min_value=18, max_value=100)
    ge_df.expect_column_values_to_match_regex("National_ID", r"^[0-9]{14}$")

    return ge_df.validate()

def validate_cards(spark_df):
    ge_df = SparkDFDataset(spark_df)

    ge_df.expect_column_values_to_not_be_null("Card_id")
    ge_df.expect_column_values_to_match_regex("Card_number", r"^[0-9]{12,19}$")
    ge_df.expect_column_values_to_be_in_set("Card_type", ["DEBIT", "CREDIT", "PREPAID"])
    ge_df.expect_column_values_to_be_between("Current_amount", min_value=0, max_value=1000000)
    ge_df.expect_column_values_to_be_between("account_age_days", min_value=0, max_value=3650)
    ge_df.expect_column_values_to_be_in_set("account_type", ["SAVINGS", "CHECKING", "BUSINESS"])

    return ge_df.validate()

def validate_devices(spark_df):
    ge_df = SparkDFDataset(spark_df)

    ge_df.expect_column_values_to_not_be_null("Dev_id")
    ge_df.expect_column_values_to_match_regex("Dev_Ip_Location", r"^\d{1,3}(\.\d{1,3}){3}$")
    ge_df.expect_column_values_to_not_be_null("Dev_name")
    ge_df.expect_column_values_to_be_in_set("Dev_type", ["MOBILE", "DESKTOP", "TABLET", "POS"])

    return ge_df.validate()


