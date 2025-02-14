"""Generic errors for module DBHelper available for subprojects
"""
# group of errors related to access to elements en DB.
# The errors are organized by module.group.action.error. That means:
# - module: In this case is the DB module (1)
# - group: The group of functions: 
#        * 0: Login
#        * 1: Users
#        * 2: Companies
#        * 8: O&M   

# - action: this is the action that cause the error:
#        * 0: Get (single)
#        * 1: Add
#        * 2: Modify
#        * 3: Delete
#        * 4: from here new functions (filtered, multiple, and so)
# - error: The errors regarding to DB are 0, regarding to generic not handles exception are 1 and 
#          the errors regarding field that must be and they are not, etc, begins on 2.... and forward

#O&M

class DBHErrors():
    #USERS
    ERR_1111= ['1.1.1.1', 'AddUser','Generic error adding a new user to DB: %s']
    ERR_1141= ['1.1.4.1', 'get','Error retrieving list of users for company: %s']
    ERR_112= ['1.1.2.0', 'reset_password_by_email','User doesn´t exists or is inactive %s']
    ERR_113= ['1.1.3.0', 'change_password','Error changing password for user %s']


