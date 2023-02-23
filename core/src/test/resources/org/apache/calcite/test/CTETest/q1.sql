SELECT sup.name, eng.name
FROM (SELECT e.name, e.salary, d.name as dname
      FROM emps e
      INNER JOIN depts d ON e.deptno = d.deptno) eng,
     (SELECT e.name, e.salary, d.name as dname
      FROM emps e
      INNER JOIN depts d ON e.deptno = d.deptno) sup
WHERE sup.salary > eng.salary
  AND eng.dname = 'Engineering'
  AND sup.dname = 'Support'