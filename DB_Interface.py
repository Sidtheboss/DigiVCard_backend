from hashlib import sha256
from io import BytesIO
import math
from fastapi import HTTPException
from fastapi.responses import JSONResponse
import mysql.connector
import pandas as pd

# MySQL database connection
def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Akash003!",
        database="swipe"
    )

def hash_password(password: str) -> str:
    """
    Hashes a password using SHA256.
    """
    return sha256(password.encode()).hexdigest()

def create_account(data: dict):
    """
    Creates a new account in the company_logins table.
    Accepts a dictionary as input.
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        email = data["email"]
        password = data["password"]
        company_id = data["company_id"]
        company_name = data["company_name"]
        phone_number = data["phone_number"]
        role = data["role"]
        username = data["username"]

        # Check if email or username already exists
        cursor.execute("SELECT email, username FROM company_logins WHERE email = %s OR username = %s", (email, username))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Email or username already exists.")

        # Hash the password
        hashed_password = hash_password(password)

        # Insert user details into the database
        insert_query = """
        INSERT INTO company_logins (email, password, company_id, company_name, phone_number, role, username)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (email, hashed_password, company_id, company_name, phone_number, role, username))
        connection.commit()

        return {"message": "Account created successfully."}

    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def login(data: dict):
    """
    Logs in a user by validating email and password.
    Returns username, role, company_id, and user_id upon successful login.
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        email = data["email"]
        password = data["password"]

        # Fetch the hashed password and other user details from the database
        cursor.execute("""
            SELECT password, username, role, company_id, user_id 
            FROM company_logins 
            WHERE email = %s
        """, (email,))
        record = cursor.fetchone()

        if not record:
            raise HTTPException(status_code=404, detail="Email not found.")

        stored_hashed_password, username, role, company_id, user_id = record

        # Compare the input password hash with the stored hash
        if hash_password(password) != stored_hashed_password:
            raise HTTPException(status_code=401, detail="Invalid credentials.")

        # Return the required details on successful login
        return {
            "message": "Login successful.",
            "username": username,
            "role": role,
            "company_id": company_id,
            "user_id": user_id
        }

    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def get_company_users(company_id: int):
    try:
        # Connect to the MySQL database
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Query to get user_id, username, email, and role based on company_id
        query = """
        SELECT user_id, username, email, role 
        FROM company_logins 
        WHERE company_id = %s
        """
        cursor.execute(query, (company_id,))

        # Fetch the results
        users = cursor.fetchall()

        # Close the connection
        cursor.close()
        conn.close()

        if not users:
            raise HTTPException(status_code=404, detail="No users found for this company")

        return users  # Return as a list of dictionaries

    except mysql.connector.Error as e:
        # Handle any errors in database connection/query execution
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    except Exception as e:
        # Catch any other exceptions
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    
# Function to insert a new company into the companies table
def new_company(data: dict):
    """
    Inserts a new company into the companies table.
    :param data: Dictionary containing company details.
    :return: Success message on successful insertion.
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # Extract data from the dictionary
        company_name = data["company_name"]
        title = data.get("title", None)
        company_subname = data.get("company_subname", None)
        description = data.get("description", None)
        website_url = data.get("website_url", None)

        # Insert query
        query = """
        INSERT INTO companies (company_name, title, company_subname, description, website_url)
        VALUES (%s, %s, %s, %s, %s)
        """
        values = (company_name, title, company_subname, description, website_url)
        cursor.execute(query, values)
        connection.commit()

        return {"message": "Company details inserted successfully.", "company_id": cursor.lastrowid}

    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    finally:
        if connection:
            connection.close()

# Function to update company details in the companies table
def update_company_details(company_id: int, data: dict):
    """
    Updates details for a specific company in the companies table.
    :param company_id: The ID of the company to update.
    :param data: Dictionary containing updated company details.
    :return: Success message on successful update.
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # Update query
        update_fields = []
        values = []
        for key, value in data.items():
            if key in ["company_name", "title", "company_subname", "description", "website_url"]:
                update_fields.append(f"{key} = %s")
                values.append(value)

        if not update_fields:
            raise HTTPException(status_code=400, detail="No valid fields provided for update.")

        values.append(company_id)
        query = f"""
        UPDATE companies
        SET {', '.join(update_fields)}, updated_at = CURRENT_TIMESTAMP
        WHERE company_id = %s
        """
        cursor.execute(query, values)
        connection.commit()

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Company not found.")

        return {"message": "Company details updated successfully."}

    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    finally:
        if connection:
            connection.close()

def get_company_details(company_id: int):
    """
    Retrieves details of a specific company based on the company_id.
    :param company_id: The ID of the company to retrieve details for.
    :return: A dictionary containing the company details.
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)  # Return results as dictionaries

        # Query to fetch company details
        query = "SELECT * FROM companies WHERE company_id = %s"
        cursor.execute(query, (company_id,))
        company_details = cursor.fetchone()

        if not company_details:
            raise HTTPException(status_code=404, detail="Company not found.")

        return {"company_details": company_details}

    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    finally:
        if connection:
            connection.close()

def update_users(data: dict, company_id: int):
    """
    Creates or updates users' accounts based on the provided data.
    Updates if the user exists, inserts if not.
    Deletes users who are not in the incoming data.
    """
    connection = None
    try:
        print("id : ", company_id)
        print("dets : ", data)

        connection = get_db_connection()
        cursor = connection.cursor()

        # Fetch company name using company_id
        company_name = get_company_name(company_id)

        # Fetch all existing user IDs for the given company_id
        cursor.execute("""
            SELECT user_id FROM company_logins WHERE company_id = %s
        """, (company_id,))
        existing_user_ids = [row[0] for row in cursor.fetchall()]
        print("Existing User IDs: ", existing_user_ids)

        incoming_user_ids = []
        # Process incoming users and update/insert them
        for user in data["users"]:
            username = user.get("username")
            email = user.get("email")
            role = user.get("role")
            password = username  # Default password is the username

            print("username : ", username)
            print("email : ", email)
            print("role : ", role)

            # Check if the user already exists in the database
            cursor.execute("""
                SELECT user_id FROM company_logins WHERE email = %s AND company_id = %s
            """, (email, company_id))
            existing_user = cursor.fetchone()

            if existing_user:
                # Update the existing user
                update_query = """
                UPDATE company_logins 
                SET role = %s, username = %s
                WHERE email = %s AND company_id = %s
                """
                cursor.execute(update_query, (role, username, email, company_id))
                print("Old user updated..... ")
                incoming_user_ids.append(existing_user[0])  # Add updated user to incoming list
            else:
                # Insert a new user
                insert_query = """
                INSERT INTO company_logins (email, company_id, role, username, password, company_name)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (email, company_id, role, username, password, company_name))
                print("New user added..... ")
                # Append newly inserted user id to incoming_user_ids
                incoming_user_ids.append(cursor.lastrowid)

        # Find users to delete (those that are in existing_user_ids but not in incoming_user_ids)
        users_to_delete = set(existing_user_ids) - set(incoming_user_ids)
        print("Users to be deleted: ", users_to_delete)

        # Delete users who are no longer in the incoming data
        for user_id in users_to_delete:
            delete_query = """
            DELETE FROM company_logins WHERE user_id = %s AND company_id = %s
            """
            cursor.execute(delete_query, (user_id, company_id))
            print(f"Deleted user_id: {user_id}")

        # Commit the transaction
        connection.commit()

        return {"message": "Accounts processed successfully.", "company_name": company_name}

    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def get_company_name(company_id: int):
    """
    Retrieves the company name from the companies table based on the company_id.
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # Query to get the company_name based on company_id
        query = """
        SELECT company_name FROM companies WHERE company_id = %s
        """
        cursor.execute(query, (company_id,))
        company = cursor.fetchone()

        # If no company is found with the given company_id, raise an error
        if not company:
            raise HTTPException(status_code=404, detail="Company not found.")

        return company[0]  # Return the company name

    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def file_upload_new_profile(file_contents: bytes, company_id: int):
    try:
        # Read the uploaded file into memory
        df = pd.read_excel(BytesIO(file_contents))  # Read Excel file using pandas

        # Explicitly replace NaN with None
        df = df.where(pd.notnull(df), None)

        # Database connection
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get the company name for the given company_id
        company_name = get_company_name(company_id)

        for index, row in df.iterrows():
            # Convert row to dictionary
            row_data = row.to_dict()

            # Replace NaN with None explicitly for each field
            def clean_value(value):
                if isinstance(value, float) and math.isnan(value):  # Check if value is NaN
                    return None
                return value or None

            # Clean each field in the row
            profile_title = clean_value(row_data.get("profile title"))
            primary_phone = clean_value(row_data.get("primary_phone"))
            secondary_phone = clean_value(row_data.get("secondary_phone"))
            email1 = clean_value(row_data.get("primary_email"))
            email2 = clean_value(row_data.get("secondary_email"))
            address1 = clean_value(row_data.get("address"))
            city = clean_value(row_data.get("city"))
            pincode = clean_value(row_data.get("pincode"))
            country = clean_value(row_data.get("country"))

            print("Processed row:", {
                "profile_title": profile_title,
                "primary_phone": primary_phone,
                "secondary_phone": secondary_phone,
                "email1": email1,
                "email2": email2,
                "address1": address1,
                "city": city,
                "pincode": pincode,
                "country": country
            })

            # Fetch user_id using primary_phone (this should be unique in the users table)
            cursor.execute("SELECT user_id FROM users WHERE phone_number = %s", (primary_phone,))
            user_result = cursor.fetchone()

            print("User present : ", user_result)

            if not user_result:
                # If no user is found, skip or log the error
                print(f"User with phone {primary_phone} not found.")
                continue

            user_id = user_result[0]

            # Insert the data into the profiles table
            query = """
            INSERT INTO profiles 
            (user_id, profile_title, primary_phone, secondary_phone, email1, email2, address1, 
            company_name, city, pincode, country, company_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                user_id,
                profile_title,
                primary_phone,
                secondary_phone,
                email1,
                email2,
                address1,
                company_name,
                city,
                pincode,
                country,
                company_id
            ))

        # Commit the changes and close the connection
        conn.commit()
        cursor.close()
        conn.close()

        return JSONResponse(content={"message": "File successfully uploaded and data inserted."}, status_code=200)

    except Exception as e:
        print(f"Exception: {str(e)}")
        return JSONResponse(content={"message": f"Error: {str(e)}"}, status_code=400)

def search_emp(company_id: int, search_term: str):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    try:
        # Query to search for users by company_id, profile_title, or common_name
        query = """
        SELECT 
            p.profile_id,
            p.profile_title,
            u.common_name,
            p.primary_phone,
            p.email1,
            p.city,
            p.country,
            p.designation,
            p.qualification
        FROM profiles p
        JOIN users u ON u.user_id = p.user_id
        WHERE p.company_id = %s
          AND (
              p.profile_title LIKE %s  -- Search in profile title
              OR u.common_name LIKE %s  -- Search in common name
              OR p.designation LIKE %s
          )
        """
        
        # Execute the query with the company_id and search term
        cursor.execute(query, (company_id, f"%{search_term}%", f"%{search_term}%", f"%{search_term}%"))
        users = cursor.fetchall()

        return users
    
    except mysql.connector.Error as err:
        print("Database error:", err)
        raise HTTPException(status_code=400, detail=f"Database error: {err}")
    
    finally:
        cursor.close()
        connection.close()

def get_profile_data(profileID: int):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)  # Fetch results as dictionary

    try:
        query = """
            SELECT 
                u.user_id,
                u.common_name,
                p.profile_id,
                p.profile_title,
                p.primary_phone,
                p.email1,
                p.designation,
                p.qualification
            FROM Profiles p
            JOIN Users u ON p.user_id = u.user_id
            WHERE p.profile_id = %s;
        """
        cursor.execute(query, (profileID,))
        db_data = cursor.fetchone()  # fetchone() since we expect only one result with a unique profile_id

        if db_data is None:
            raise HTTPException(status_code=404, detail="Profile not found")

        return {
            "user_id": db_data['user_id'],
            "common_name": db_data['common_name'],
            "profile_id": db_data['profile_id'],
            "profile_title": db_data['profile_title'],
            "primary_phone": db_data['primary_phone'],
            "designation": db_data['designation'],
            "email": db_data['email1'],
            "qualification": db_data['qualification'],
        }

    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=f"Error: {err}")
    
    finally:
        cursor.close()
        connection.close()

def update_emp(data: dict):
    """
    Updates details for a specific profile in the profiles table.
    :param data: Dictionary containing updated profile details, including profile_id.
    :return: Success message on successful update.
    """
    connection = None
    try:
        # Ensure that 'Emp_profile_id' exists in the data dictionary
        if "Emp_profile_id" not in data:
            raise HTTPException(status_code=400, detail="Profile ID is required for update.")
        
        profile_id = data["Emp_profile_id"]
        print(data)
        # Remove 'Emp_profile_id' from data to avoid attempting to update it
        data.pop("Emp_profile_id")

        connection = get_db_connection()
        cursor = connection.cursor()

        # Mapping the dictionary keys to actual column names in the database
        column_map = {
            "Emp_title": "profile_title",
            "Emp_designation": "designation",
            "Emp_qualification": "qualification",
            "Emp_phone": "primary_phone",
            "Emp_email": "email1",
        }

        # Update query fields and values
        update_fields = []
        values = []
        for key, value in data.items():
            if key in column_map:  # Only update the valid columns
                update_fields.append(f"{column_map[key]} = %s")
                values.append(value)
        
        print(data)

        if not update_fields:
            raise HTTPException(status_code=400, detail="No valid fields provided for update.")

        # Add the profile_id as the last parameter for the WHERE clause
        values.append(profile_id)

        print(values)
        print(update_fields)

        query = f"""
        UPDATE profiles
        SET {', '.join(update_fields)}
        WHERE profile_id = %s
        """
        cursor.execute(query, values)
        connection.commit()

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Profile not found.")

        return {"message": "Profile details updated successfully."}

    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    finally:
        if connection:
            connection.close()
