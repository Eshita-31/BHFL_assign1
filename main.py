from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
from loguru import logger
from datetime import datetime
import uvicorn

app = FastAPI()

file_path = r'C:\Users\Welcome\Desktop\BHFL_a1\Assignment1.xlsx'
export_path = r'C:\Users\Welcome\Desktop\BHFL_a1\exported_data.xlsx'

def load_sheet(sheet_name):
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    df.rename(columns=lambda x: x.strip(), inplace=True)
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].astype(str).str.strip()
    return df



accounts = load_sheet('Accounts')
policies = load_sheet('Policies')
claims = load_sheet('Claims')

accounts.fillna("", inplace=True)
policies.fillna("", inplace=True)
claims.fillna("", inplace=True)



if 'AccountId' not in accounts.columns:
    raise ValueError("Accounts sheet must have 'AccountId' column.")


logger.info("Loaded DataFrames:")
logger.info(f"Accounts columns: {accounts.columns.tolist()}")
logger.info(f"Policies columns: {policies.columns.tolist()}")
logger.info(f"Claims columns: {claims.columns.tolist()}")


history_columns = ["timestamp", "operation", "table", "primary_key", "old_data", "new_data"]
history_df = pd.DataFrame(columns=history_columns)


DB = {
    "accounts": accounts,
    "policies": policies,
    "claims": claims,
    "history": history_df
}


class Customer(BaseModel):
    AccountId: str
    Name: str
    Age: int
    City: str
    State: str
    Pincode: int

class Policy(BaseModel):
    HAN: str
    PolicyName: str
    AccountId: Optional[str] = None

class Claim(BaseModel):
    Id: str
    CreatedDate: str
    CaseNumber: str
    HAN: Optional[str] = None
    BillAmount: float
    Status: str
    AccountId: Optional[str] = None

class CustomerResponse(BaseModel):
    customer: Customer
    policies: List[Policy]
    claims: List[Claim]

def add_history_entry(operation: str, table: str, primary_key: str, old_data: dict, new_data: dict):
    global DB
    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "operation": operation,
        "table": table,
        "primary_key": primary_key,
        "old_data": str(old_data),
        "new_data": str(new_data)
    }
    DB["history"] = pd.concat([DB["history"], pd.DataFrame([entry])], ignore_index=True)

@app.get("/")
async def read_root():
    return {"message": "API is running"}


@app.get("/customers/{account_id}", response_model=CustomerResponse)
async def get_customer(account_id: str):
    try:
        accounts_df = DB["accounts"]
        policies_df = DB["policies"]
        claims_df = DB["claims"]

        customer_data = accounts_df[accounts_df['AccountId'] == account_id]
        if customer_data.empty:
            raise HTTPException(status_code=404, detail="Customer not found")

        if 'AccountId' in policies_df.columns:
            policies_data = policies_df[policies_df['AccountId'] == account_id]
        else:
            policies_data = pd.DataFrame()

        if 'AccountId' in claims_df.columns:
            claims_data = claims_df[claims_df['AccountId'] == account_id]
        else:
            claims_data = pd.DataFrame()

        customer_dict = customer_data.to_dict(orient="records")[0]
        policies_list = policies_data.to_dict(orient="records")
        claims_list = claims_data.to_dict(orient="records")

        customer_obj = Customer(**customer_dict)
        policies_obj = [Policy(**p) for p in policies_list]
        claims_obj = [Claim(**c) for c in claims_list]

        return {
            "customer": customer_obj,
            "policies": policies_obj,
            "claims": claims_obj
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching customer: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/customers/", response_model=Customer)
async def create_customer(customer: Customer):
    try:
        accounts_df = DB["accounts"]
        if customer.AccountId in accounts_df['AccountId'].values:
            raise HTTPException(status_code=400, detail="Customer already exists")
        new_customer = pd.DataFrame([customer.dict()])
        old_data = {}
        new_data = customer.dict()
        DB["accounts"] = pd.concat([accounts_df, new_customer], ignore_index=True)
        add_history_entry("CREATE", "Accounts", customer.AccountId, old_data, new_data)
        return customer
    except Exception as e:
        logger.error(f"Error creating customer: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.put("/customers/{account_id}", response_model=Customer)
async def update_customer(account_id: str, customer: Customer):
    try:
        accounts_df = DB["accounts"]
        index = accounts_df[accounts_df['AccountId'] == account_id].index
        if index.empty:
            raise HTTPException(status_code=404, detail="Customer not found")

        old_data = accounts_df.loc[index].to_dict(orient='records')[0]
        new_data = customer.dict()
        for col, val in new_data.items():
            DB["accounts"].at[index[0], col] = val

        add_history_entry("UPDATE", "Accounts", account_id, old_data, new_data)

        return customer
    except Exception as e:
        logger.error(f"Error updating customer: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.delete("/customers/{account_id}", response_model=dict)
async def delete_customer(account_id: str):
    try:
        accounts_df = DB["accounts"]
        policies_df = DB["policies"]
        claims_df = DB["claims"]

        index = accounts_df[accounts_df['AccountId'] == account_id].index
        if index.empty:
            raise HTTPException(status_code=404, detail="Customer not found")

        old_data_customer = accounts_df.loc[index].to_dict(orient='records')[0]

        if 'AccountId' in policies_df.columns:
            pol_index = policies_df[policies_df['AccountId'] == account_id].index
            old_policies = policies_df.loc[pol_index].to_dict(orient='records')
            DB["policies"] = policies_df.drop(pol_index)
            for p in old_policies:
                add_history_entry("DELETE", "Policies", p.get('HAN',''), p, {})

        if 'AccountId' in claims_df.columns:
            claims_index = claims_df[claims_df['AccountId'] == account_id].index
            old_claims = claims_df.loc[claims_index].to_dict(orient='records')
            DB["claims"] = claims_df.drop(claims_index)
            for c in old_claims:
                add_history_entry("DELETE", "Claims", c.get('Id',''), c, {})

        DB["accounts"] = accounts_df.drop(index)
        add_history_entry("DELETE", "Accounts", account_id, old_data_customer, {})

        return {"message": "Customer deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting customer: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/policies/", response_model=Policy)
async def create_policy(policy: Policy):
    try:
        policies_df = DB["policies"]
        if 'HAN' in policies_df.columns and policy.HAN in policies_df['HAN'].values:
            raise HTTPException(status_code=400, detail="Policy already exists")

        if policy.AccountId and not DB["accounts"][DB["accounts"]['AccountId'] == policy.AccountId].empty:
            new_policy = pd.DataFrame([policy.dict()])
            old_data = {}
            new_data = policy.dict()
            DB["policies"] = pd.concat([policies_df, new_policy], ignore_index=True)
            add_history_entry("CREATE", "Policies", policy.HAN, old_data, new_data)
            return policy
        elif policy.AccountId is None:
            new_policy = pd.DataFrame([policy.dict()])
            old_data = {}
            new_data = policy.dict()
            DB["policies"] = pd.concat([policies_df, new_policy], ignore_index=True)
            add_history_entry("CREATE", "Policies", policy.HAN, old_data, new_data)
            return policy
        else:
            raise HTTPException(status_code=400, detail="Associated AccountId not found")
    except Exception as e:
        logger.error(f"Error creating policy: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.put("/policies/{han}", response_model=Policy)
async def update_policy(han: str, policy: Policy):
    try:
        policies_df = DB["policies"]
        if 'HAN' not in policies_df.columns:
            raise HTTPException(status_code=404, detail="No HAN column in Policies")
        index = policies_df[policies_df['HAN'] == han].index
        if index.empty:
            raise HTTPException(status_code=404, detail="Policy not found")

        old_data = policies_df.loc[index].to_dict(orient='records')[0]
        new_data = policy.dict()

        if policy.AccountId and DB["accounts"][DB["accounts"]['AccountId'] == policy.AccountId].empty:
            raise HTTPException(status_code=400, detail="Invalid AccountId for this policy")

        for col, val in new_data.items():
            DB["policies"].at[index[0], col] = val

        add_history_entry("UPDATE", "Policies", han, old_data, new_data)
        return policy
    except Exception as e:
        logger.error(f"Error updating policy: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.delete("/policies/{han}", response_model=dict)
async def delete_policy(han: str):
    try:
        policies_df = DB["policies"]
        index = policies_df[policies_df['HAN'] == han].index
        if index.empty:
            raise HTTPException(status_code=404, detail="Policy not found")

        old_data = policies_df.loc[index].to_dict(orient='records')[0]
        DB["policies"] = policies_df.drop(index)

        add_history_entry("DELETE", "Policies", han, old_data, {})
        return {"message": "Policy deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting policy: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/claims/", response_model=Claim)
async def create_claim(claim: Claim):
    try:
        claims_df = DB["claims"]
        if 'Id' in claims_df.columns and claim.Id in claims_df['Id'].values:
            raise HTTPException(status_code=400, detail="Claim already exists")

        if claim.AccountId and not DB["accounts"][DB["accounts"]['AccountId'] == claim.AccountId].empty:
            new_claim = pd.DataFrame([claim.dict()])
            old_data = {}
            new_data = claim.dict()
            DB["claims"] = pd.concat([claims_df, new_claim], ignore_index=True)
            add_history_entry("CREATE", "Claims", claim.Id, old_data, new_data)
            return claim
        elif claim.AccountId is None:
            new_claim = pd.DataFrame([claim.dict()])
            old_data = {}
            new_data = claim.dict()
            DB["claims"] = pd.concat([claims_df, new_claim], ignore_index=True)
            add_history_entry("CREATE", "Claims", claim.Id, old_data, new_data)
            return claim
        else:
            raise HTTPException(status_code=400, detail="Associated AccountId not found")
    except Exception as e:
        logger.error(f"Error creating claim: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.put("/claims/{id}", response_model=Claim)
async def update_claim(id: str, claim: Claim):
    try:
        claims_df = DB["claims"]
        if 'Id' not in claims_df.columns:
            raise HTTPException(status_code=404, detail="No Id column in Claims")
        index = claims_df[claims_df['Id'] == id].index
        if index.empty:
            raise HTTPException(status_code=404, detail="Claim not found")

        old_data = claims_df.loc[index].to_dict(orient='records')[0]
        new_data = claim.dict()

        if claim.AccountId and DB["accounts"][DB["accounts"]['AccountId'] == claim.AccountId].empty:
            raise HTTPException(status_code=400, detail="Invalid AccountId for this claim")

        for col, val in new_data.items():
            DB["claims"].at[index[0], col] = val

        add_history_entry("UPDATE", "Claims", id, old_data, new_data)
        return claim
    except Exception as e:
        logger.error(f"Error updating claim: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.delete("/claims/{id}", response_model=dict)
async def delete_claim(id: str):
    try:
        claims_df = DB["claims"]
        index = claims_df[claims_df['Id'] == id].index
        if index.empty:
            raise HTTPException(status_code=404, detail="Claim not found")

        old_data = claims_df.loc[index].to_dict(orient='records')[0]
        DB["claims"] = claims_df.drop(index)

        add_history_entry("DELETE", "Claims", id, old_data, {})
        return {"message": "Claim deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting claim: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/export/")
async def export_data():
    try:
        with pd.ExcelWriter(export_path) as writer:
            DB["accounts"].to_excel(writer, sheet_name="Accounts", index=False)
            DB["policies"].to_excel(writer, sheet_name="Policies", index=False)
            DB["claims"].to_excel(writer, sheet_name="Claims", index=False)
            DB["history"].to_excel(writer, sheet_name="History", index=False)

        return {"message": "Data exported successfully", "path": export_path}
    except Exception as e:
        logger.error(f"Error exporting data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
