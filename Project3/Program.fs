open System
open Akka
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open System.Collections.Generic
open ChordNode

// Configuration
let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {            
            stdout-loglevel : ERROR
            loglevel : ERROR
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
        }")

(*
type Node(x: int, y:int) as this =
    let id =x
    let other = y
    member this.GetId() = x

let a = [Node(4,5)]

for i in a do
    printfn "%d" (i.GetId())
*)

type MainCommands =
    | StartAlgorithm of (int*int)
    | Create of (int*IActorRef)
    | Notify of (int*IActorRef)
    | Stabilize
    | FindSuccessor of (int*IActorRef)
    | TellYourPredecessor
    | Join2 of (int*int)
    | PredecessorResponse of (int*IActorRef)
    | PredecessorRequest
    | SuccessorOf of (int*IActorRef)

let chordSystem = ActorSystem.Create("ChordSystem", configuration)
let mutable mainActorRef = null

let mutable numNodes = 0
let mutable numRequests = 0
let mutable m = 6
let mutable firstNodeRef = null
let mutable secondNodeRef = null
let StabilizeCycletimeMs = 500.0

let mutable hashSpace = pown 2 m |> int

(*
type FingerTableEntry(x:int, y:IActorRef) as this =
    let id = x
    let idRef = y
    member this.GetId() = x
*)


let ChordNode (myId:int) (mailbox:Actor<_>) =    
   // let mutable hashSpace = pown(2, m) |> int
    let mutable firstNode = 0
    let mutable mySuccessor = 0
    let mutable mySuccessorRef = null
    let mutable myPredecessor = 0
    let mutable myPredecessorRef = null
   // let mutable myFingerTable = []

    let rec loop () = 
        actor {
            let! (message) = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with 
            | StartAlgorithm(numNodes, numRequests) ->
                firstNode <- Random().Next(m)
            | Create (otherId, otherRef) ->
                // First two nodes in the Chord Ring
                mySuccessor <- otherId
                myPredecessor <- otherId
                mySuccessorRef <- otherRef
                myPredecessorRef <- otherRef
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(StabilizeCycletimeMs), mailbox.Self, Stabilize)

            | Notify(predecessorId, predecessorRef) ->
                //if predecessorId > myPredecessor then
                myPredecessor <- predecessorId
                myPredecessorRef <- predecessorRef

            | Stabilize ->
                // Ask successor for its predecessor and wait
                mySuccessorRef <! PredecessorRequest

            | PredecessorResponse(predecessorOfSuccessor, itsRef) ->                    
                if predecessorOfSuccessor <> myId then
                    mySuccessor <- predecessorOfSuccessor
                    mySuccessorRef <- itsRef
                printfn "\n %d p = %d s = %d" myId myPredecessor mySuccessor
                // Notify mysuccessor
                mySuccessorRef <! Notify(myId, mailbox.Self)
                
            | PredecessorRequest->    
                // Find my successor ref and send Notify
                sender <! PredecessorResponse(myPredecessor, myPredecessorRef)

            | SuccessorOf(isId, isRef) ->
                mySuccessor <- isId
                mySuccessorRef <- isRef
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(StabilizeCycletimeMs), mailbox.Self, Stabilize)
                printfn "\n %d found successor = %d" myId mySuccessor
                mySuccessorRef <! Notify(myId, mailbox.Self)

            | FindSuccessor(newId, seekerRef) ->
                if mySuccessor < myId && (newId > myId || newId < mySuccessor) then 
                    seekerRef <! SuccessorOf(mySuccessor, mySuccessorRef)
                    printfn "\n %d (last node) Successor of %d is %d" myId newId mySuccessor
                elif newId <= mySuccessor && newId > myId then 
                    seekerRef <! SuccessorOf(mySuccessor, mySuccessorRef)
                    printfn "\n %d Successor of %d is %d" myId newId mySuccessor
                else 
                    mySuccessorRef <! FindSuccessor(newId, seekerRef)
                    // CLOSEST PRECEDING NODE
                    (*for x in m .. 1 do
                        if (myFingerTable.[x] >=< (myId, newId)) then 
                            // find ref of actor myFingerTable[x] and send FindSuccessor to it
                            printfn "\n %d is telling %d to find successor for %d" myId myFingerTable.[x] newId*)

            | _ -> ()

            return! loop()
        }
    loop()


let MainActor (mailbox:Actor<_>) =    
    let mutable firstNodeId = 0
    let mutable secondNodeId = 0
    let mutable tempNodeId = 0
    let mutable tempNodeRef = null

    let rec loop () = 
        actor {
            let! (message) = mailbox.Receive()

            match message with 
            | StartAlgorithm(numNodes, numRequests) ->
                //firstNodeId <- Random().Next(hashSpace)
                firstNodeId <- 42
                firstNodeRef <- spawn chordSystem (sprintf "%d" firstNodeId) (ChordNode firstNodeId)
                // Second Node
                //secondNodeId <- Random().Next(hashSpace)
                secondNodeId <- 8
                secondNodeRef <- spawn chordSystem (sprintf "%d" secondNodeId) (ChordNode secondNodeId)
                firstNodeRef <! Create(secondNodeId, secondNodeRef)
                secondNodeRef <! Create(firstNodeId, firstNodeRef)

                System.Threading.Thread.Sleep(10000)                // add remaining nodes
                //tempNodeId <- Random().Next(1, hashSpace)
                tempNodeId <- 30
                printfn "\n\n ADDING %d" tempNodeId
                tempNodeRef <- spawn chordSystem (sprintf "%d" tempNodeId) (ChordNode tempNodeId)
                firstNodeRef <! FindSuccessor(tempNodeId, tempNodeRef)

                System.Threading.Thread.Sleep(800)
                tempNodeId <- 55
                //tempNodeId <- Random().Next(1, hashSpace)
                printfn "\n\n ADDING %d" tempNodeId
                tempNodeRef <- spawn chordSystem (sprintf "%d" tempNodeId) (ChordNode tempNodeId)
                firstNodeRef <! FindSuccessor(tempNodeId, tempNodeRef) 


                System.Threading.Thread.Sleep(900)
                tempNodeId <- 61
                //tempNodeId <- Random().Next(1, hashSpace)
                printfn "\n\n ADDING %d" tempNodeId
                tempNodeRef <- spawn chordSystem (sprintf "%d" tempNodeId) (ChordNode tempNodeId)
                firstNodeRef <! FindSuccessor(tempNodeId, tempNodeRef) 
                
            | _ -> ()

            return! loop()
        }
    loop()


[<EntryPoint>]
let main argv =
    numNodes <-  argv.[0] |> int
    numRequests <- argv.[1] |> int

    mainActorRef <- spawn chordSystem "MainActor" MainActor
    mainActorRef <! StartAlgorithm(numNodes, numRequests)

    chordSystem.WhenTerminated.Wait()
        
    0