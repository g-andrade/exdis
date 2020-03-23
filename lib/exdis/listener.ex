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

  def ip_address_and_port() do
    {inet_ip_address, port} = :ranch.get_addr(@listener_ref)
    ip_address_charlist = :inet.ntoa(inet_ip_address)
    ip_address = List.to_string(ip_address_charlist)
    {ip_address, port}
  end
end
