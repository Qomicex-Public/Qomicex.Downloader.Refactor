namespace Qomicex.Downloader.Refactor;

public class Container
{
    private readonly Dictionary<Type, object> _instances = new();

    public Container Register<T>(T instance) where T : class
    {
        _instances[typeof(T)] = instance;
        return this;
    }

    public Container Register<TInterface, TImpl>(TImpl instance) where TImpl : class, TInterface
    {
        _instances[typeof(TInterface)] = instance;
        return this;
    }

    public T Resolve<T>()
    {
        if (_instances.TryGetValue(typeof(T), out var instance))
            return (T)instance;
        throw new InvalidOperationException($"类型 {typeof(T).Name} 未注册");
    }
}
