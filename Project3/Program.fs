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

let chordSystem = ActorSystem.Create("ChordSystem", configuration)

type MainCommands =
    | StartAlgorithm of (int*int)
    | Create of (int*IActorRef)
    | Notify of (int*IActorRef)
    | Stabilize
    | FindNewNodeSuccessor of (int*IActorRef)
    | FoundNewNodeSuccessor of (int*IActorRef)
    | PredecessorRequest
    | PredecessorResponse of (int*IActorRef)
    | KeyLookup of (int*int*int)
    | FixFingers
    | FindithSuccessor of (int*int*IActorRef)
    | FoundFingerEntry of (int*int*IActorRef)
    | StartLookups of (int)
    | FoundKey of (int)
    

let mutable mainActorRef = null

let mutable numNodes = 0
let mutable numRequests = 0
let mutable m =6
let mutable firstNodeRef = null
let mutable secondNodeRef = null
let StabilizeCycletimeMs = 50.0
let FixFingersCycletimeMs = 250.0

let mutable hashSpace = pown 2 m |> int

let updateElement index element list = 
  list |> List.mapi (fun i v -> if i = index then element else v)

type FingerTableEntry(x:int, y:IActorRef) as this =
    let id = x
    let idRef = y
    member this.GetId() = x
    member this.GetRef() = y


// Tracks and prints average hop count 
let PrinterActor (mailbox: Actor<_>) =
    let mutable hopCountSum = 0
    let mutable requestsCount = 0
    let endCondition = numNodes * numRequests

    let rec loop () =
        actor {
            let! (message) = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with 
            | FoundKey (hopCount) ->
                hopCountSum <- hopCountSum + hopCount
                requestsCount <- requestsCount + 1

                if requestsCount = endCondition then 
                    let avgHopCount = float(hopCountSum)/float(requestsCount)
                    printfn "\n AVERAGE HOPCOUNT = %.2f" avgHopCount
            | _ -> ()

            return! loop()

        }
    loop ()


let printerRef = spawn chordSystem "PrinterActor" PrinterActor  


let ChordNode (myId:int) (mailbox:Actor<_>) =    
   // let mutable hashSpace = pown(2, m) |> int
    let mutable firstNode = 0
    let mutable mySuccessor = 0
    let mutable mySuccessorRef = null
    let mutable myPredecessor = 0
    let mutable myPredecessorRef = null
    let mutable myFingerTable = []
    let a = FingerTableEntry(0, null)
    let myFingerTable : FingerTableEntry[] = Array.create m a

    let rec loop () = 
        actor {
            let! (message) = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with 
            | Create (otherId, otherRef) ->
                // First two nodes in the Chord Ring
                mySuccessor <- otherId
                myPredecessor <- otherId
                mySuccessorRef <- otherRef
                myPredecessorRef <- otherRef
                for i in 0..m-1 do
                    let tuple = FingerTableEntry(mySuccessor, mySuccessorRef)
                    myFingerTable.[i] <- tuple
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(StabilizeCycletimeMs), mailbox.Self, Stabilize)
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(FixFingersCycletimeMs), mailbox.Self, FixFingers)

            | Notify(predecessorId, predecessorRef) ->
                //if predecessorId > myPredecessor then
                myPredecessor <- predecessorId
                myPredecessorRef <- predecessorRef

            | FixFingers ->
                let mutable ithFinger = 0
                for i in 1..m-1 do
                    ithFinger <- ( myId + ( pown 2 i ) ) % hashSpace
                    mailbox.Self <! FindithSuccessor(i, ithFinger, mailbox.Self)

            | FindithSuccessor(i, key, tellRef) ->
                if mySuccessor < myId && (key > myId || key < mySuccessor) then
                    tellRef <! FoundFingerEntry(i, mySuccessor, mySuccessorRef)
                elif key <= mySuccessor && key > myId then 
                    tellRef <! FoundFingerEntry(i, mySuccessor, mySuccessorRef)
                elif myId > key then 
                    let ithRef = myFingerTable.[m-1].GetRef()
                    ithRef <! FindithSuccessor(i, key, tellRef)
                else 
                    let mutable Break = false 
                    let mutable x = m
                    while not Break do
                        x <- x - 1
                        if x < 0 then   
                            mySuccessorRef <! FindithSuccessor(i, key, tellRef)
                            Break <- true
                        else
                            let ithFinger = myFingerTable.[x].GetId()
                            if (ithFinger > myId && ithFinger <= key) then 
                                let ithRef = myFingerTable.[x].GetRef()
                                ithRef <! FindithSuccessor(i, key, tellRef)
                                Break <- true                       
                    done                 

            | FoundFingerEntry(i, fingerId, fingerRef) ->
                let tuple = FingerTableEntry(fingerId, fingerRef)
                myFingerTable.[i] <- tuple

            | Stabilize ->
                if mySuccessor <> 0 then 
                    mySuccessorRef <! PredecessorRequest

            | PredecessorResponse(predecessorOfSuccessor, itsRef) ->                    
                if predecessorOfSuccessor <> myId then
                    mySuccessor <- predecessorOfSuccessor
                    mySuccessorRef <- itsRef
                // Notify mysuccessor
                mySuccessorRef <! Notify(myId, mailbox.Self)
                
            | PredecessorRequest->    
                sender <! PredecessorResponse(myPredecessor, myPredecessorRef)

            | FoundNewNodeSuccessor(isId, isRef) ->
                // Update successor information of self
                printfn "\n %d ka s = %d" myId isId
                mySuccessor <- isId
                mySuccessorRef <- isRef
                // populate fingertable entry with successor - it will get corrected in next FixFingers call
                for i in 0..m-1 do
                    let tuple = FingerTableEntry(mySuccessor, mySuccessorRef)
                    myFingerTable.[i] <- tuple
                // start Stabilize scheduler
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(0.0),TimeSpan.FromMilliseconds(StabilizeCycletimeMs), mailbox.Self, Stabilize)
                // start FixFingers scheduler
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(0.0),TimeSpan.FromMilliseconds(FixFingersCycletimeMs), mailbox.Self, FixFingers)
                // Notify Successor
                mySuccessorRef <! Notify(myId, mailbox.Self)
         
            | KeyLookup(key, hopCount, initiatedBy) ->
                if mySuccessor < myId && (key > myId || key < mySuccessor) then
                    printfn "\n initiatedBy = %d key = %d at = %d hopCount = %d" initiatedBy key mySuccessor hopCount
                elif myId > key then 
                    let ithRef = myFingerTable.[m-1].GetRef()
                    ithRef <! KeyLookup(key, hopCount + 1, initiatedBy)
                elif key <= mySuccessor && key > myId then 
                    printfn "\n initiatedBy = %d key = %d at = %d hopCount = %d" initiatedBy key mySuccessor hopCount
                else 
                    let mutable Break = false 
                    let mutable i = m-1
                    while not Break do  
                        let ithFinger = myFingerTable.[i].GetId()
                        if (ithFinger > myId && ithFinger <= key) then 
                            let ithRef = myFingerTable.[i].GetRef()
                            ithRef <! KeyLookup(key, hopCount + 1, initiatedBy)
                            Break <- true 
                        i <- i - 1
                        if i < 0 then   
                            mySuccessorRef <! KeyLookup(key, hopCount + 1, initiatedBy)
                            Break <- true
                    done  
                
            | StartLookups(numRequests) ->
                printf "\n %d Starting lookups" myId
                let mutable tempKey = 0
                if mySuccessor <> firstNode then 
                    mySuccessorRef <! StartLookups(numRequests)
                for x in 1..numRequests do
                    tempKey <- Random().Next(1, hashSpace)
                    mailbox.Self <! KeyLookup(tempKey, 1, myId)
                    System.Threading.Thread.Sleep(800)


            | FindNewNodeSuccessor(newId, seekerRef) ->
                if mySuccessor < myId && (newId > myId || newId < mySuccessor) then 
                    seekerRef <! FoundNewNodeSuccessor(mySuccessor, mySuccessorRef)
                    printfn "\n %d (last node) Successor of %d is %d" myId newId mySuccessor
                elif newId <= mySuccessor && newId > myId then 
                    seekerRef <! FoundNewNodeSuccessor(mySuccessor, mySuccessorRef)
                    printfn "\n %d Successor of %d is %d" myId newId mySuccessor
                else 
                    mySuccessorRef <! FindNewNodeSuccessor(newId, seekerRef)

            | _ -> ()

            return! loop()
        }
    loop()

let MainActor (mailbox:Actor<_>) =    
    let mutable firstNodeId = 0
    let mutable secondNodeId = 0
    let mutable tempNodeId = 0
    let mutable tempNodeRef = null
    let mutable tempKey = 0

    let rec loop () = 
        actor {
            let! (message) = mailbox.Receive()

            match message with 
            | StartAlgorithm(numNodes, numRequests) ->
                firstNodeId <- Random().Next(hashSpace)
                printfn "\n\n ADDING %d" firstNodeId
                firstNodeRef <- spawn chordSystem (sprintf "%d" firstNodeId) (ChordNode firstNodeId)
                // Second Node
                secondNodeId <- Random().Next(hashSpace)
                printfn "\n\n ADDING %d" secondNodeId
                secondNodeRef <- spawn chordSystem (sprintf "%d" secondNodeId) (ChordNode secondNodeId)
                firstNodeRef <! Create(secondNodeId, secondNodeRef)
                secondNodeRef <! Create(firstNodeId, firstNodeRef)

                for x in 3..numNodes do
                    System.Threading.Thread.Sleep(1000)
                    tempNodeId <- Random().Next(1, hashSpace)
                    printfn "\n\n ADDING %d" tempNodeId
                    tempNodeRef <- spawn chordSystem (sprintf "%d" tempNodeId) (ChordNode tempNodeId)
                    firstNodeRef <! FindNewNodeSuccessor(tempNodeId, tempNodeRef)
                
                printfn "\n Ring stabilized"
                System.Threading.Thread.Sleep(8000)
                firstNodeRef <! StartLookups(numRequests)

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