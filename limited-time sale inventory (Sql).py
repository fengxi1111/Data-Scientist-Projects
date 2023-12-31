#!/usr/bin/env python
# coding: utf-8

# # Pessimistic Locking for Inventory Update

# In[ ]:


# begin
connection.begin()

# lock (FOR UPDATE) read current stock record
cursor.execute("SELECT quantity FROM inventory WHERE id = ? FOR UPDATE", item_id)
current_quantity = cursor.fetchone()['quantity']

# check the stock whether enough
if current_quantity >= requested_quantity:
    new_quantity = current_quantity - requested_quantity
    
    cursor.execute("UPDATE inventory SET quantity = ? WHERE id = ?" (new_quantity, item_id))
    
    #write down detailed execution
    
    connection.commit()
    return True

else:
    connetcion.rollback()
    return False  


# # Optimistic Locking for Inventory Update

# In[ ]:


connection.begin()
 
cursor.execute("SELECT quantity, version FROM inventory WHERE id = ?", item_id)
result = cursor.fetchone()
current_quantity = result['quantity']
current_version = result['version']
 
if current_quantity >= requested_quantity:

    new_quantity = current_quantity - requested_quantity
    new_version = current_version + 1
 
    cursor.execute("UPDATE inventory SET quantity=%s,version=%s WHERE id=%s AND version=%s"
    	,(new_quantity, new_version, item_id, current_version))
 
    #write down detailed execution
 
    # check updated row
    if cursor.rowcount == 1:
        connection.commit()
        return True
    
    else:
        # if update fail, maybe version not match
        connection.rollback()
        return False
else:
    connection.rollback()
    return False


# # Preemptive check perspective

# In[ ]:


cursor.execute("SELECT quantity FROM inventory WHERE id = ? ", item_id)
current_quantity = cursor.fetchone()['quantity']
if current_quantity >= requested_quantity:
    connection.begin()
    #update stock   
    cursor.execute("UPDATE inventory SET quantity = quantity-? WHERE id = ? and quantity - ?>= 0", (requested_quantity, item_id,requested_quantity))
    
    #write down detailed execution

    if cursor.rowcount == 1:
        connection.commit()
        return True
    else:
        connection.rollback()
        return False
 
    else:
        return False 

