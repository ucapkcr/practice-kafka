# Run this to clear out the development cluster being used locally

from confluent_kafka.admin import AdminClient, NewTopic

import settings


def reset_kafka(admin_client: AdminClient) -> None:
    # Get topics that currently exist
    cluster_metadata = admin_client.list_topics()
    topics = [k for k in cluster_metadata.topics.keys()]

    print('Topics before deletion: {}'.format(topics))

    # Delete all the topics
    try:
        deletion = admin_client.delete_topics(topics, operation_timeout=settings.STANDARD_TIMEOUT)

        # Wait for confirmation
        for topic, f in deletion.items():
            try:
                f.result()
                print('Topic {} has been deleted'.format(topic))
            
            except Exception as e:
                print('Failed to delete topic {}: {}'.format(topic, e))

        cluster_metadata = admin_client.list_topics()
        topics = [k for k in cluster_metadata.topics.keys()]

        print('Topics after deletion: {}'.format(topics))
    
    except ValueError as e:
        print('No topics to be deleted')

    # Get consumer groups
    consumer_groups_future = admin_client.list_consumer_groups()

    try:
        consumer_groups_result = consumer_groups_future.result()
        consumer_groups = [c.group_id for c in consumer_groups_result.valid]
    
    except Exception as e:
        print('Something went wrong: {}'.format(e))
    
    print('Consumer groups before deletion: {}'.format(consumer_groups))
    
    # Delete all consumer groups - doesn't always seem necessary, might be an eventual consistency thing
    try:
        deletion = admin_client.delete_consumer_groups(consumer_groups)

        # Wait for confirmation
        for cg, f in deletion.items():
            try:
                f.result()
                print('Consumer group {} has been deleted'.format(cg))
            
            except Exception as e:
                print('Failed to delete consumer group {}: {}'.format(cg, e))

        # Get consumer groups
        consumer_groups_future = admin_client.list_consumer_groups()

        try:
            consumer_groups_result = consumer_groups_future.result()
            consumer_groups = [c.group_id for c in consumer_groups_result.valid]
        
        except Exception as e:
            print('Something went wrong: {}'.format(e))
        
        print('Consumer groups after deletion: {}'.format(consumer_groups))
    
    except ValueError as e:
        print('No consumer groups to be deleted')

    return

def prepare_kafka(admin_client: AdminClient) -> None:
    # Prepare topics to be created
    topics = [NewTopic(t, 1, 1) for t in settings.TOPIC_NAMES]

    print('Topics to be created: {}'.format(topics))

    # Create topics
    admin_client.create_topics(topics)

    print('Topics created.')

    return

if __name__ == '__main__':
    admin_client = AdminClient({
        "bootstrap.servers": settings.KAFKA_HOST
    })

    reset_kafka(admin_client)
    prepare_kafka(admin_client)
