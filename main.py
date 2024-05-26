import os

from azure.identity import DefaultAzureCredential
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.resource import ResourceManagementClient
from dotenv import load_dotenv

from src.models.azure import ROLE, AzureRelationship, AzureResource
from src.services.graphdb import AzureGraphDB, GraphDBConfig

load_dotenv()


def extract_data(subscription_id, resource_group_name) -> tuple[list, list]:

    # Authenticate using DefaultAzureCredential
    credential = DefaultAzureCredential()

    # Initialize ResourceManagementClient
    resource_client = ResourceManagementClient(credential, subscription_id)

    # Initialize AuthorizationManagementClient
    authorization_client = AuthorizationManagementClient(
        credential, subscription_id)

    # Get all resources in the specified resource group
    resource_list = list(
        resource_client.resources.list_by_resource_group(resource_group_name))

    # Collect resource details
    nodes = []
    relationships = []
    for resource in resource_list:
        print(resource)
        print("\n\n")
        nodes.append(AzureResource(
            id=resource.id,
            name=resource.name,
            type=resource.type,
            location=resource.location
        ))

        if resource.identity:
            nodes.append(AzureResource(
                id=resource.identity.principal_id,
                name=resource.identity.principal_id,
                type="Service Principal",
                location=resource.location,
            ))

            relationships.append(AzureRelationship(
                source=resource.identity.principal_id,
                target=resource.id,
                relationship_type=ROLE.ASSIGNED_TO,
            ))

    role_assignments = authorization_client.role_assignments.list_for_scope(
        scope=f'/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}'
    )

    for assignment in role_assignments:
        print(assignment)
        print("\n\n")
        nodes.append(AzureResource(
            id=assignment.principal_id,
            name=assignment.name,
            type=assignment.principal_type,
            location=resource.location,
        ))

            # print(assignment)
            # break

            # relationships.append(AzureRelationship(
            #     source=assignment.principal_id,
            #     target=resource.id,
            #     relationship_type=ROLE.ASSIGNED_TO,
            # ))

        relationships.append(AzureRelationship(
            source=assignment.principal_id,
            target=assignment.scope,
            relationship_type=ROLE.HAS_ROLE,
            extra_properties={
                "role_definition_id": assignment.role_definition_id
            }
        ))

    return nodes, relationships


if __name__ == "__main__":

    subscription_id = os.getenv("SUBSCRIPTION_ID")
    resource_group_name = os.getenv("RESOURCE_GROUP")

    dbconfig = GraphDBConfig(
        URL=os.getenv("GRAPHDB_URL"),
        USERNAME=os.getenv("GRAPHDB_USERNAME"),
        PASSWORD=os.getenv("GRAPHDB_PASSWORD")
    )
    azgraph = AzureGraphDB(dbconfig)
    azgraph.clear()

    nodes, relationships = extract_data(subscription_id, resource_group_name)

    for node in nodes:
        azgraph.create_node(node)

    for relationship in relationships:
        try:
            azgraph.create_edge(relationship)
        except Exception as e:
            print(e)
