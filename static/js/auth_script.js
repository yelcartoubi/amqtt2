function toggleClient(clientID, isEnabled) {
    // Prepare the data to be sent to the server
    const data = {
        client_id: clientID,
        enable: isEnabled === 'true' ? 'false' : 'true'
    };

    // Send an AJAX request to the server to update the client's status
    fetch('/toggle_client', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(data),
    })
    .then(response => response.json())
    .then(data => {
        console.log('Success:', data);
        // Reload the page or update the UI accordingly
        location.reload();
    })
    .catch((error) => {
        console.error('Error:', error);
    });
}