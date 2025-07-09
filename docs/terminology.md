# Terminology

| Term            | Meaning                                                                                                                                                                              | 
|-----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Engine          | The actual database engine implementation we are talking to.                                                                                                                         |                                                            
| Driver          | The client driver infrastructure used to talk to an **Engine**.                                                                                                                      |
| Connection      | A thread owned, stateful network connection going via a **Driver** to an **Engine**                                                                                                  |
| Statement       | String executed against a **Driver**. Statements can be DDL, DML or just regular SQL queries or configuration control                                                                |  
| Instrumentation | Details collected, either while running or after execution, about the execution of a **Statement**                                                                                   |
| Query           | A **Statement** along with all the **Instrumentation** gathered about it                                                                                                             |
| Result          | The thing which comes back from a **Statement**. results can be iterated                                                                                                             |
| SQL Type        | A common, unified, ANSI inspired type that all supported **Drivers** can support. **Connections** can translate **SQL Type** to **Engine** specific types                            | 
| Explain AST     | A unified Abstract Syntax Tree representing how a **Statement** is going to execute on a **Connection**                                                                              |
| Analyse AST     | An **Explain AST** decorated with the **Instrumentation** that came from the **Query** containing the **Statement** executed                                                         |
| Plan Node       | The nodes make up the **Explain AST** or **Analyse AST**                                                                                                                             |
| Theorem         | One or more **Queries** executed against any **Driver** that together allows us to prove if features being presentin the **Engine** or measurement about the quality of that feature |
