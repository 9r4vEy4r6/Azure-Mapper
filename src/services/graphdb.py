from neo4j import GraphDatabase, ManagedTransaction

from src.models.azure import ROLE, AzureRelationship, AzureResource


class GraphDBConfig:
    URL: str = None
    USERNAME: str = None
    PASSWORD: str = None

    def __init__(self, URL: str, USERNAME: str, PASSWORD: str) -> None:
        self.URL = URL
        self.USERNAME = USERNAME
        self.PASSWORD = PASSWORD


class GraphDB:
    def __init__(self, config: GraphDBConfig) -> None:
        self.config = config
        self.driver = GraphDatabase.driver(
            config.URL, auth=(config.USERNAME, config.PASSWORD))
        self.driver.verify_connectivity()

    def close(self) -> None:
        self.driver.close()

    def clear(self) -> None:
        with self.driver.session() as session:
            session.write_transaction(
                lambda tx: tx.run("MATCH ()-[r]-() DELETE r"))
            session.write_transaction(lambda tx: tx.run("MATCH (n) DELETE n"))


class AzureGraphDB(GraphDB):
    def __init__(self, config: GraphDBConfig) -> None:
        super().__init__(config)

    def check_node(self, id: str) -> bool:
        def _check_node(tx: ManagedTransaction, id: str) -> bool:
            result = tx.run(
                "MATCH (n:NODE {id : $id}) RETURN n LIMIT 1", id=id)
            result = result.single()
            return result != None

        with self.driver.session() as session:
            result = session.read_transaction(_check_node, id)
            return result

    def create_node(self, resource: AzureResource) -> None:
        if self.check_node(resource.id):
            print(f"Node exists {resource.name}")
            return

        def _create_node(tx: ManagedTransaction, resource: AzureResource):
            result = tx.run(
                "CREATE (n:NODE $resource) RETURN n", resource=resource.__dict__)
            # Check if node got created
            result = tx.run(
                "MATCH (n:NODE {id : $id}) RETURN n LIMIT 1", id=resource.id)
            result = result.single()
            assert result != None
            return result

        with self.driver.session() as session:
            result = session.write_transaction(_create_node, resource)
            print(f"Created node {resource.name}")

    def create_edge(self, relationship: AzureRelationship) -> None:
        def _create_edge(tx: ManagedTransaction, relationship: AzureRelationship):
            # Check if nodes exist to create edge
            assert self.check_node(
                relationship.source), f"Source node {relationship.source} for edge does not exist"
            assert self.check_node(
                relationship.target), f"Destination node {relationship.target} for edge does not exist"

            if relationship.relationship_type == ROLE.ASSIGNED_TO:
                result = tx.run("""
                        MATCH (nf:NODE {id : $source})
                        MATCH (nt:NODE {id : $target})
                        MERGE (nf)-[r:ASSIGNED_TO]->(nt)""",
                                source=relationship.source, target=relationship.target)
            elif relationship.relationship_type == ROLE.HAS_ROLE:
                result = tx.run("""
                        MATCH (nf:NODE {id : $source})
                        MATCH (nt:NODE {id : $target})
                        MERGE (nf)-[r:HAS_ROLE {role_definition_id: $role_definition_id}]->(nt)""",
                                source=relationship.source, target=relationship.target, role_definition_id=relationship.extra_properties["role_definition_id"])
            print(
                f"Created edge between {relationship.source} and {relationship.target}")

        with self.driver.session() as session:
            session.write_transaction(_create_edge, relationship)
