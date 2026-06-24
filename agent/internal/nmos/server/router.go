package server

import "net/http"

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET "+nodeBasePath+"{$}", s.env.HandleNodeVersions)
	mux.HandleFunc("GET "+nodeV13Path+"{$}", s.env.HandleNode)
	mux.HandleFunc("GET "+nodeV13Path+"self", s.env.HandleNode)
	mux.HandleFunc("GET "+nodeV13Path+"devices", s.env.HandleDevices)
	mux.HandleFunc("GET "+nodeV13Path+"sources", s.env.HandleSources)
	mux.HandleFunc("GET "+nodeV13Path+"flows", s.env.HandleFlows)
	mux.HandleFunc("GET "+nodeV13Path+"senders", s.env.HandleSenders)
	mux.HandleFunc("GET "+nodeV13Path+"receivers", s.env.HandleReceivers)

	mux.HandleFunc("GET "+connectionBasePath+"{$}", s.env.HandleConnectionVersions)
	mux.HandleFunc("GET "+connectionV12Path+"{$}", s.env.HandleConnectionVersions)
	mux.HandleFunc("GET "+connectionSenderV12Path+"{senderID}/active", s.env.HandleSenderActive)
	mux.HandleFunc("GET "+connectionSenderV12Path+"{senderID}/staged", s.env.HandleSenderStaged)
	mux.HandleFunc("PATCH "+connectionSenderV12Path+"{senderID}/staged", s.env.HandleSenderStaged)
	mux.HandleFunc("GET "+connectionSenderV12Path+"{senderID}/constraints", s.env.HandleSenderConstraints)
	mux.HandleFunc("GET "+connectionSenderV12Path+"{senderID}/transportfile", s.env.HandleSenderTransportFile)

	return LoggingMiddleware(s.opts.Logger)(
		RecoverMiddleware(s.opts.Logger)(
			CORSMiddleware(
				ContentTypeMiddleware(
					PreserveMuxErrorMiddleware(mux),
				),
			),
		),
	)
}
