 
Download the datasets from Kaggle and load them into pandas DataFrames

    q1 = q1_transactions_6_to_23_hours_gt_15[["transaction_id", "final_amount"]].sort_values(by=["transaction_id"])
    q2_1 = q2_best_selling_with_name[["year_month_created_at", "item_name", "sellings_qty"]]
    q2_2 = q2_most_profits_with_name[["year_month_created_at", "item_name", "profit_sum"]]
    q3 = q3_tpv_with_name[["year_half_created_at", "store_name", "tpv"]].sort_values(by=["year_half_created_at", "store_name"])
    q4 = q4_most_purchases_with_store_and_user[["store_name", "birthdate"]].sort_values(by=["store_name", "birthdate"])
    
    q1.to_csv("results_query_1.csv", index=False)
    q2_1.to_csv("results_query_2_1.csv", index=False)
    q2_2.to_csv("results_query_2_2.csv", index=False)
    q3.to_csv("results_query_3.csv", index=False)
    q4.to_csv("results_query_4.csv", index=False)
