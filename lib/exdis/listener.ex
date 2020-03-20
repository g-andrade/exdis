defmodule Exdis.Listener do
  ## ------------------------------------------------------------------
  ## Macro-like Attribute Definitions
  ## ------------------------------------------------------------------

  @listener_ref __MODULE__

  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def child_spec([]) do
    ref = @listener_ref
    transport = :ranch_tcp
    transport_opts = [port: 7369]
    protocol = Exdis.Connection
    protocol_opts = %{}
    :ranch.child_spec(
      ref, transport, transport_opts,
      protocol, protocol_opts)
  end
end
