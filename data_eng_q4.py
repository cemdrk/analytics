from pathlib import Path
import pandas as pd
import argparse
import sys


class Config:
    MIN_DATE_DEFAULT = "2021-01-08"
    MAX_DATE_DEFAULT = "2021-05-30"
    TOP_DEFAULT = 5
    DATA_FOLDER = Path(__file__).parent / "input_data" / "data"


rolling_win_7_mean = lambda d: d.rolling(window=7).mean()


def parse_args(argv: [str], config: Config = Config):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--min-date",
        help=f"start of the date range. type:str, format:'YYYY-MM-DD', default:'{config.MIN_DATE_DEFAULT}'",
        default=config.MIN_DATE_DEFAULT,
        type=str,
    )
    parser.add_argument(
        "--max-date",
        help=f"end of the date range. type:str, format:'YYYY-MM-DD', default:'{config.MAX_DATE_DEFAULT}'",
        default=config.MAX_DATE_DEFAULT,
        type=str,
    )
    parser.add_argument(
        "--top",
        help=f"number of rows in the WMAPE output. type:int, default:{config.TOP_DEFAULT}",
        type=int,
        default=config.TOP_DEFAULT,
    )

    args = parser.parse_args(argv)

    return args


def get_product_features(data: pd.DataFrame) -> pd.DataFrame:
    sales_product_grouped = data.groupby(["store_id", "product_id", "brand_id", "date"])
    sales_product = sales_product_grouped["quantity"].sum()
    ma_7_p = sales_product.transform(rolling_win_7_mean)
    lag_7_p = sales_product.shift(periods=7)

    product_aggregated = ma_7_p.to_frame().rename(columns={"quantity": "MA7_P"})
    product_aggregated["sales_product"] = sales_product
    product_aggregated["LAG7_P"] = lag_7_p
    return product_aggregated


def get_brand_features(data:pd.DataFrame)->pd.DataFrame:
    sales_brand_grouped = data.groupby(["brand_id", "store_id", "date"])
    sales_brand = sales_brand_grouped["quantity"].sum()
    ma7_b = sales_brand.transform(rolling_win_7_mean)
    lag7_b = sales_brand.shift(periods=7)

    brand_aggregated = ma7_b.to_frame().rename(columns={"quantity": "MA7_B"})
    brand_aggregated["LAG7_B"] = lag7_b
    brand_aggregated["sales_brand"] = sales_brand
    return brand_aggregated


def get_store_features(data:pd.DataFrame)->pd.DataFrame:
    store_brand_grouped = data.groupby(["store_id", "date"])
    sales_store = store_brand_grouped["quantity"].sum()
    ma7_s = sales_store.transform(rolling_win_7_mean)
    lag7_s = sales_store.shift(periods=7)

    store_aggregated = ma7_s.to_frame().rename(columns={"quantity": "MA7_S"})
    store_aggregated["LAG7_S"] = lag7_s
    store_aggregated["sales_store"] = sales_store
    return store_aggregated


def to_csv(data:pd.DataFrame, min_date:str, max_date:str)->None:
    filtered = data[(data["date"] >= min_date) & (data["date"] <= max_date)]
    columns = [
        "product_id",
        "store_id",
        "brand_id",
        "date",
        "sales_product",
        "MA7_P",
        "LAG7_P",
        "sales_brand",
        "MA7_B",
        "LAG7_B",
        "sales_store",
        "MA7_S",
        "LAG7_S",
    ]
    filtered.to_csv("features.csv", columns=columns, index=False)


def main():
    args = parse_args(sys.argv[1:])
    min_date, max_date, top = args.min_date, args.max_date, args.top

    products = pd.read_csv(Config.DATA_FOLDER / "product.csv")
    sales = pd.read_csv(Config.DATA_FOLDER / "sales.csv")
    brands = pd.read_csv(Config.DATA_FOLDER / "brand.csv")
    stores = pd.read_csv(Config.DATA_FOLDER / "store.csv")

    products.rename(
        columns={"id": "product_id"},
        inplace=True,
    )
    brands.rename(
        columns={"id": "brand_id", "name": "brand"},
        inplace=True,
    )
    stores.rename(
        columns={"id": "store_id"},
        inplace=True,
    )
    sales.rename(
        columns={"product": "product_id", "store": "store_id"},
        inplace=True,
    )

    product_brand_df = products.merge(brands, on="brand")
    sales_brand_df = sales.merge(product_brand_df, on="product_id")

    # get features
    product_features = get_product_features(sales_brand_df)
    brand_features = get_brand_features(sales_brand_df)
    store_features = get_store_features(sales_brand_df)

    # merge
    sales_brand_df = sales_brand_df.merge(
        product_features, on=["store_id", "product_id", "brand_id", "date"]
    )
    sales_brand_df = sales_brand_df.merge(
        brand_features, on=["brand_id", "store_id", "date"]
    )
    sales_brand_df = sales_brand_df.merge(store_features, on=["store_id", "date"])

    to_csv(sales_brand_df, min_date, max_date)


if __name__ == "__main__":
    main()
