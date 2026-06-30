I want to make changes in my dw artitecture :
1. delete all source_..._id or source_..._code from facts I don't need them in facts 
2. also delete source system from all fact and dimentions

3. in fact_donation_lifecycle delete days_to_confirm , days_to_allocate field
and add min_donation, max_donation, avg_donation

4. delete dim_allocation_type because we don't have in source and delete refrence key to this dim  in fact also 

5. in fact_expense_transaction delete description

6. in event fact delete ( allocated_amount , reason ,source_allocation_id) because fact-less fact mush just show relationship it shouldn't have any  measure 
==================================================================

now edit etl that  

===================================================================


2. 
