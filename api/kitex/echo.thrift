namespace go echo

// EchoService is the minimal service used by the Kitex example.
// It shows how to promote this template to Kitex for service-to-service RPC.
service EchoService {
    string Echo(1: string message)
}
