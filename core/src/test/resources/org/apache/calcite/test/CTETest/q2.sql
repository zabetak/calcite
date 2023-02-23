WITH cte AS (SELECT e.name, e.salary, d.name as dname
      FROM emps e
      INNER JOIN depts d ON e.deptno = d.deptno)
SELECT sup.name, eng.name
FROM cte eng,
     cte sup
WHERE sup.salary > eng.salary
  AND eng.dname = 'Engineering'
  AND sup.dname = 'Support'