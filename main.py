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
        nodes.append(AzureResource(
            id=resource.id,
            name=resource.name,
            type=resource.type,
            location=resource.location
        ))

    for resource in resource_list:
        # Get role assignments for the resource's system assigned managed identity
        if resource.identity:
            role_assignments = authorization_client.role_assignments.list_for_scope(
                scope=f'/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}',
                filter=f"assignedTo('{resource.identity.principal_id}')"
            )
            role_assignments = list(role_assignments)

            for assignment in role_assignments:
                nodes.append(AzureResource(
                    id=assignment.principal_id,
                    name=assignment.name,
                    type=assignment.principal_type,
                    location=resource.location,
                ))

                relationships.append(AzureRelationship(
                    source=assignment.principal_id,
                    target=resource.id,
                    relationship_type=ROLE.ASSIGNED_TO,
                ))

                relationships.append(AzureRelationship(
                    source=assignment.principal_id,
                    target=assignment.scope,
                    relationship_type=ROLE.HAS_ROLE,
                    extra_properties={
                        "role_definition_id": assignment.role_definition_id
                    }
                ))

        # Check for user assigned identities
        if resource.identity and resource.identity.user_assigned_identities:
            for id, obj in resource.identity.user_assigned_identities.items():
                # Link the user assigned managed identity to the resource
                relationships.append(AzureRelationship(
                    source=id.replace("resourcegroups", "resourceGroups"),
                    target=resource.id,
                    relationship_type=ROLE.ASSIGNED_TO,
                ))
        
        # If the resource is a user assigned managed identity, link it to the resources
        if resource.type == "Microsoft.ManagedIdentity/userAssignedIdentities":
            role_assignments = authorization_client.role_assignments.list_for_scope(
                scope=resource.id
            )
            role_assignments = list(role_assignments)
            for assignment in role_assignments:
                print(assignment)
                # Create a node for management group
                nodes.append(AzureResource(
                    id=assignment.scope,
                    name=assignment.name,
                    type="Microsoft.Management/managementGroups",
                    location=resource.location,
                ))

                relationships.append(AzureRelationship(
                    source=assignment.scope,
                    target=resource.id,
                    relationship_type=ROLE.ASSIGNED_TO,
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
