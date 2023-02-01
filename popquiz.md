# Pop quiz for my team! (For the prize of a starbucks drink)

- What is the naive implementation of this algorithm susceptible to in terms of serving requests in a real world
    distributed systems' scenario? -> No duplication, so if a server goes down, we lose the state.
- What category does this architecture fall into in terms of cap theorem ? -> Strongly Consistent, we do not 
duplicate data across servers and so we cannot tolerate network partition.
