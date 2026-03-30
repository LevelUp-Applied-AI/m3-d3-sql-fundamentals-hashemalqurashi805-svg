import sqlite3

# --- Task 1: Aggregation ---
def top_departments(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = """
    SELECT d.name, SUM(e.salary) as total_salary
    FROM departments d
    JOIN employees e ON d.dept_id = e.dept_id
    GROUP BY d.name
    ORDER BY total_salary DESC
    LIMIT 3;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results

# --- Task 2: The Join ---
def employees_with_projects(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # تم تعديل p.project_name إلى p.name بناءً على الخطأ السابق
    query = """
    SELECT e.name AS employee_name, p.name AS project_name
    FROM employees e
    JOIN project_assignments pa ON e.emp_id = pa.emp_id
    JOIN projects p ON pa.project_id = p.project_id;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results

# --- Task 3: Window Function ---
def salary_rank_by_department(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # هذا الاستعلام يحسب رتبة راتب الموظف داخل قسمه
    query = """
    SELECT e.name AS employee_name, d.name AS dept_name, e.salary,
           RANK() OVER (PARTITION BY e.dept_id ORDER BY e.salary DESC) as salary_rank
    FROM employees e
    JOIN departments d ON e.dept_id = d.dept_id;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results

# --- Main Execution Area ---
if __name__ == "__main__":
    db_file = 'drill.db'
    
    print("\n--- Task 1: Top 3 Departments ---")
    try:
        data1 = top_departments(db_file)
        for row in data1:
            print(f"Dept: {row[0]} | Total Salary: {row[1]}")
    except Exception as e:
        print(f"Error Task 1: {e}")

    print("\n--- Task 2: Employees with Projects ---")
    try:
        data2 = employees_with_projects(db_file)
        for row in data2:
            print(f"Employee: {row[0]} | Project: {row[1]}")
    except Exception as e:
        print(f"Error Task 2: {e}")

    print("\n--- Task 3: Salary Rank by Department ---")
    try:
        data3 = salary_rank_by_department(db_file)
        for row in data3:
            print(f"Rank {row[3]}: {row[0]} ({row[1]}) - Salary: {row[2]}")
    except Exception as e:
        print(f"Error Task 3: {e}")