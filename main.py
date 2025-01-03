from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from DB_Interface import create_account, download_profiles_as_excel, file_upload_new_profile, get_company_details, get_company_users, get_profile_data, login, new_company, search_emp, update_company_auth_status, update_company_details, update_emp, update_employee_auth_status, update_users

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

@app.post("/create-account")
async def create_account_endpoint(request: Request):
    try:
        data = await request.json()
        return create_account(data)
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"Missing required field: {str(e)}")

@app.post("/login")
async def login_endpoint(request: Request):
    try:
        data = await request.json()
        return login(data)
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"Missing required field: {str(e)}")
    
@app.post("/add-company")
async def login_endpoint(request: Request):
    try:
        data = await request.json()
        return new_company(data)
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"Missing required field: {str(e)}")

@app.get("/get-users")
async def cards(data: int = Query(...)):
    user_data = get_company_users(data)
    return user_data

@app.get("/get-company")
async def cards(data: int = Query(...)):
    company_data = get_company_details(data)
    return company_data

@app.post("/update-company")
async def updateCompany(request: Request, data: int = Query(...)):
    company_dets = await request.json()
    user_data = update_company_details(data, company_dets)
    return user_data

@app.post("/update-user")
async def update_company(request: Request, data: int = Query(...)):
    """
    Update the users' data for the specified company_id.
    """
    try:
        # Parse the JSON body
        company_dets = await request.json()
        print("Received company details: ", company_dets)  # Debugging print

        # Call the update_users function with company details and company_id (data)
        user_data = update_users(company_dets, data)
        return user_data

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing request: {e}")

@app.post("/upload-file")
async def upload_file(file: UploadFile = File(...), data: int = Query(...)):
    try:
        # Read the uploaded file into memory
        contents = await file.read()

        # Call the function to process the file and insert data
        return file_upload_new_profile(contents, data)

    except Exception as e:
        return JSONResponse(content={"message": f"Error: {str(e)}"}, status_code=400)

@app.get("/search-emp")
def searchFriends(company_id:int, search_query: str):
    try:
        emps = search_emp(company_id, search_query)  # Call the function to search users by name
        return emps
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Error searching users: {err}")
    
@app.get("/profile-data")
async def profile(data: int = Query(...)):
    profile_data = get_profile_data(data)
    return profile_data

@app.post("/update-emp")
async def updateCompany(request: Request):
    company_dets = await request.json()
    user_data = update_emp(company_dets)
    return user_data

@app.get("/download-profiles")
def download_profiles(company_id: int = Query(...)):
    return download_profiles_as_excel(company_id)

@app.post("/auth-company")
async def cards(data: int = Query(...)):
    user_data = update_company_auth_status(data)
    return user_data

@app.post("/auth-employee")
async def cards(data: int = Query(...)):
    user_data = update_employee_auth_status(data)
    return user_data
