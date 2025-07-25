username="igogosec@2020"
password="igogosec@2021"

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager  # Auto-download ChromeDriver


# Auto-setup ChromeDriver (no manual downloads needed)
driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install())

try:
    driver.get("https://prems.necta.go.tz/prems/")
    
    # Wait for login fields
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "username")))
    
    # Enter credentials
    driver.find_element(By.ID, "username").send_keys(username)
    driver.find_element(By.ID, "password").send_keys(password)
    driver.find_element(By.ID, "submit").click()  # Adjust if submit button has a different ID
    
    # Check if login succeeded (URL contains "dashboard")
    WebDriverWait(driver, 10).until(lambda d: "dashboard" in d.current_url.lower())
    print("✅ Login successful! Dashboard loaded.")
    
except Exception as e:
    print("❌ Error:", e)
    print("Current URL:", driver.current_url)  # Debugging help
    
finally:
    driver.quit()