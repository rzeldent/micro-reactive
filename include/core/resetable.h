template <typename T>
class IResetable
{
public:
    virtual void Reset() = 0;

protected:
    virtual ~IResetable() = default;
};

template <typename T>
class Resetable : public IResetable<T>
{
protected:
    bool _isComplete = false;

public:
    void Reset()
    {
        _isComplete = false;
    }
};