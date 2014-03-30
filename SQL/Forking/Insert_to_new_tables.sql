-- A PL/SQL to insert from an one set of tables (Old set) to another (New set)
-- This primarily came about because of additions of new columns and changes to the order of columns
-- in the Staging tables, which otherwise reflect the Clarity-Tapestry strcuture
-- As there is no DDL to modify a table for column order, a new set has to be created.
-- This PL/SQL helps populate the new set from the old (or existing).

DECLARE
   insert_SQL VARCHAR2(32767);
   new_table_name VARCHAR2(256);
   select_SQL VARCHAR2(1000);
   row_count PLS_INTEGER := 0;
BEGIN
   FOR table_detail IN
      ( select table_name
              ,wm_concat(column_name) as column_list
          from user_tab_columns
         where table_name like 'NWTAP\_%' escape '\'
         group by table_name
         order by table_name
      )
   LOOP
      DBMS_OUTPUT.PUT_LINE('Starting on Table=' || table_detail.table_name);
      new_table_name := 'C2_' || table_detail.table_name;
      
      -- Minimial restart logic. Skip the new tables that already have rows in it.
      select_SQL := 'SELECT count(1) FROM ' || new_table_name || ' WHERE ROWNUM < 2';
      EXECUTE IMMEDIATE select_SQL INTO row_count;

      IF (row_count = 0) THEN
         insert_SQL := 'INSERT INTO ' || new_table_name || ' (' || table_detail.column_list || 
         			') SELECT ' || table_detail.column_list || ' FROM ' || table_detail.table_name;
         EXECUTE IMMEDIATE insert_SQL;
         DBMS_OUTPUT.PUT_LINE('Table=' || table_detail.table_name || ', Rows Inserted=' || SQL%ROWCOUNT);
         COMMIT;
      END IF;
   END LOOP;
END;
/
