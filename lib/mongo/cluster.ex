defmodule Mongo.Cluster do
  use GenServer

  @behaviour DBConnection.Pool

  alias Mongo.{ConnectionError, Protocol, Topology}

  defstruct [
    :opts,
    :pool_module,
    :topology_pid
  ]

  def ensure_all_started(opts, type) do
    {pool_module, opts} = Keyword.pop(opts, :underlying_pool)
    pool_module.ensure_all_started(opts, type)
  end

  def start_link(Protocol, opts) do
    {pool_module, opts} = Keyword.pop(opts, :underlying_pool)
    {name, opts} = Keyword.pop(opts, :name)
    opts = Keyword.put(opts, :pool, pool_module)
    {:ok, topology_pid} = Topology.start_link(opts)

    state = %__MODULE__{
      opts: opts,
      pool_module: pool_module,
      topology_pid: topology_pid
    }

    GenServer.start_link(__MODULE__, state, name: name)
  end

  def child_spec(module, opts, child_opts) do
    Supervisor.Spec.worker(__MODULE__, [module, opts], child_opts)
  end

  def checkout(cluster, opts) do
    case GenServer.call(cluster, {:checkout, opts}) do
      {:ok, conn, pool_module} ->
        with {:ok, pool_ref, module, state} <- pool_module.checkout(conn, opts) do
          {:ok, {pool_module, pool_ref}, module, state}
        end
    end
  end

  def checkin({pool_module, pool_ref}, state, opts) do
    pool_module.checkin(pool_ref, state, opts)
  end

  def disconnect({pool_module, pool_ref}, err, state, opts) do
    pool_module.disconnect(pool_ref, err, state, opts)
  end

  def stop({pool_module, pool_ref}, err, state, opts) do
    pool_module.stop(pool_ref, err, state, opts)
  end

  def handle_call({:checkout, opts}, _from, %__MODULE__{} = state) do
    %{pool_module: pool_module} = state

    case Topology.select_server(state.topology_pid, opts) do
      {:ok, conn, _slave_ok, _mongos} ->
        {:reply, {:ok, conn, pool_module}, state}
    end
  end
end
