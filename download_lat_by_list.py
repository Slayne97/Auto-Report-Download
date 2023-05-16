import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException

# Constants
USER_BOX_ID = "ContentPlaceHolder1_LogIn2_txtLogInUserNameValue"
PASS_BOX_ID = "ContentPlaceHolder1_LogIn2_txtLogInPasswordValue"
USER = "548048"
PASSWORD = "1974"
LAT_LINK = r'http://apps.rymx.trw.com/QA/LAT/Pages/LogIn.aspx'


DROPDOWN_ID = "ContentPlaceHolder1_ContentPlaceHolder1_" \
              "Report_Ticket1_rptvTicketDetail_ReportToolbar_ExportGr_FormatList_DropDownList"
export_button_id = 'ContentPlaceHolder1_ContentPlaceHolder1_Report_Ticket1_rptvTicketDetail_ReportToolbar_ExportGr_Export'
TICKET_BOX_NAME = 'ctl00$ctl00$ContentPlaceHolder1$ContentPlaceHolder1$Report_Ticket1$txtTicketName'

# Selenium configuration
options = Options()
options.add_experimental_option('detach', True)


def click_on(some_driver, element):
    some_driver.execute_script("arguments[0].click();", element)


def get_ticket_list(file_path: str) -> list:
    df = pd.read_html(file_path)[0]
        
    print(df)
    
    list_of_tickets = set(df['Name'].to_list())
    list_of_tickets = list(list_of_tickets)
    print(list_of_tickets)
    return list_of_tickets


# Function to log in
def log_in(_driver):
    user_box = _driver.find_element(By.ID, USER_BOX_ID)
    user_box.send_keys(USER)
    pass_box = _driver.find_element(By.ID, PASS_BOX_ID)
    pass_box.send_keys(PASSWORD)
    pass_box.send_keys(Keys.ENTER)

    # Click "Reporte por Ticket"
    reporte_por_ticket = _driver.find_element(By.XPATH, "//a[@href='Report_Ticket.aspx']")
    click_on(_driver, reporte_por_ticket)
    # reporte_por_ticket.click()


def click_download(_driver, _ticket):
    wait = WebDriverWait(_driver, 10)

    ticket_box = _driver.find_element(By.NAME, TICKET_BOX_NAME)

    # Insert that ticket into the textbox and click enter
    ticket_box.clear()
    ticket_box.send_keys(_ticket)
    ticket_box.send_keys(Keys.ENTER)

    wait.until_not(EC.element_to_be_clickable((By.ID, DROPDOWN_ID)))
    wait.until(EC.element_to_be_clickable((By.ID, DROPDOWN_ID)))

    # Select from the dropdown
    export_dropdown = Select(_driver.find_element(By.ID, DROPDOWN_ID))
    export_dropdown.select_by_value('XLS')

    # Download
    wait.until(EC.element_to_be_clickable((By.ID, export_button_id)))
    export_button = _driver.find_element(By.ID, export_button_id)
    click_on(_driver, export_button)
    # export_button.click()


if __name__ == '__main__':
    ticket_list = get_ticket_list(r"C:\Users\Z0205784\Downloads\ReportedeLAT2023-04-11.xls")

    driver = webdriver.Chrome(r"C:\Software\chromedriver.exe", options=options)
    driver.get(LAT_LINK)

    log_in(driver)

    count = 0
    print(len(ticket_list))

    for ticket in ticket_list:
        print(count, ticket)
        try:
            click_download(driver, ticket)
        except NoSuchElementException:
            driver.back()
            continue
        except StaleElementReferenceException:
            driver.refresh()
            continue
        count += 1

