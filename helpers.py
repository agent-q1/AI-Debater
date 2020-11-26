from neo4j import GraphDatabase

driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "suhas"))

def add_friend(tx, name, friend_name):
    tx.run("MERGE (a:Person {name: $name}) "
           "MERGE (a)-[:KNOWS]->(friend:Person {name: $friend_name})",
           name=name, friend_name=friend_name)

def print_friends(tx, name):
    for record in tx.run("MATCH (a:Person)-[:KNOWS]->(friend) WHERE a.name = $name "
                         "RETURN friend.name ORDER BY friend.name", name=name):
        print(record["friend.name"])

def load_csv(tx):

    for record in tx.run("LOAD CSV WITH HEADERS FROM 'file:///desktop-csv-import/test_arguments.csv' AS row "                         
                         "WITH toInteger(row.Index) as index, row.Argument as argument "
                         "MERGE (a:Argument {index: index})"
                         "ON CREATE SET a.Argument = argument"
                         ):

        print(record["index"])
    
    for record in tx.run("LOAD CSV WITH HEADERS FROM 'file:///desktop-csv-import/test_relations.csv' AS row "
                         "WITH toInteger(row.attacked) as attackedID , toInteger(row.attacking) as attackingID, toFLoat(row.attack) as attackValue "
                         "MATCH (a:Argument {index: attackedID}) "
                         "MATCH (b: Argument {index: attackingID}) "
                         "MERGE (a)-[r:RATTACK {attackValue: attackValue}]->(b)" 
                         ""
                         "" 
                         ):
        print(record["attackedID"] , record["attackingID"] , record["attackValue"])      

def delete_all(tx):
    tx.run("MATCH (a:Argument)-[r:RATTACK]->(b:Argument) DELETE a,r,b" )  
    tx.run("MATCH (a:Argument) DELETE a")                



def print_arguments(tx):
    for record in tx.run("MATCH (a:Argument) "
                         "RETURN a "):
        print(record["a"])


def find_next(tx, index):
    result = tx.run("MATCH (a: Argument {index: $index})"
                    "MATCH (b: Argument)"
                    "MATCH (a)-[r:RATTACK ]->(b)"
                    "RETURN r.attackValue AS attackval, b as attackedNode" , index = index)

    
    maxutil = -100.0

    maxattackId = index

    

    for record in result:
        attackedNode = record["attackedNode"]
        attackval = record["attackval"]
        # print( "attacked node ", attackedNode)
        # print("attack value " ,  attackval)
        if(attackval==None):
            continue
        attackId = attackedNode["index"]
        # print("Attack id: ")
        # print(attackId)
        util =  attackval - driver.session().read_transaction(find_path,attackId)
        # print("util at ", util)
        if(util > maxutil):
            maxutil = util
            # print("max at attack id")
            # print(attackId)
            # print(maxutil)
            maxattackId = attackId

    
    print("Finally max utility is ===================>     ", maxutil  )
    print("Max node at ", maxattackId)

    return maxattackId
        
        
def find_weakness(tx, index = 1):
    result = tx.run("MATCH (a: Argument {index: $index} ) "
                    "MATCH (b: Argument ) "
                    "MATCH (a)-[r: RATTACK]->(b) "
                    "RETURN sum(r.attackValue) as weakness",index = index)

    for record in result:
        return record["weakness"]

        


    
def find_path(tx, index = 1 , depth = 1):
    paths = []
    result =  tx.run("MATCH (a: Argument {index: $index}) "
                         "MATCH (b: Argument) "
                         "MATCH p = (a)-[:RATTACK *1 ]->(b) "
                         "RETURN relationships(p) AS path " , index = index, depth = depth)
    for record in result:

        paths.append(record["path"])

    utility = 0.0
    maxutil = 0.0

    # print(paths)

    
    
    
    
    for path in paths:
        counter = 0;
        utility = 0.0
        # curnode
        # print(path)
        for r in path:
            
            # print(r["attackValue"])
            if(r["attackValue"]==None):
                continue

            if(counter%2==0):
                utility= utility + r["attackValue"]
            else:
                utility=  utility- r["attackValue"]
            
            
            counter+=1
            # print(counter)
        if(counter==1):
            # print("tralala")
            if(maxutil<utility): 
                maxutil = utility
        # print(maxutil)

    # print(maxutil)

    return maxutil

def find_next_argument(tx, index):
    next_index = find_next(tx, index)
    result = tx.run("MATCH (a:Argument {index: $next_index}) RETURN a.Argument " , next_index = next_index)
    for record in result:
        print(record["a.Argument"])
    print(next_index)

with driver.session() as session:

    # session.write_transaction(delete_all)
    # session.write_transaction(load_csv)
    
    session.read_transaction(find_next_argument, 1)


driver.close()