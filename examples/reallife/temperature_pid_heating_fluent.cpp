#include <micro-reactive.h>
#include <random>
#include <functional>

#if defined(ARDUINO)
#include <Arduino.h>
#else
#error "This example only works on Arduino platform"
#endif

using namespace rx;

/**
 * Real-life Example: Temperature PID Heating Control (Fluent API)
 * 
 * Same as temperature_pid_heating.cpp but using the fluent API.
 * With hysteresis to prevent rapid switching.
 * 
 * Hardware: ESP32/Arduino compatible
 * Library: Micro-Reactive
 */

// Simple logger observer for demo purposes
template<typename T>
class SimpleLogger : public IObserver<T> {
private:
    T last_value_;
    bool has_value_ = false;
    int count_ = 0;

public:
    void OnNext(const T& value) override {
        last_value_ = value;
        has_value_ = true;
        count_++;
    }
    void OnCompleted() override {}
    void OnError(const std::exception&) override {}
    T GetLastValue() const { return last_value_; }
    bool HasValue() const { return has_value_; }
    int GetCount() const { return count_; }
};

// Simulated temperature sensor that produces noisy readings
class TemperatureSensor {
private:
    double current_temp_;
    double target_temp_;
    std::mt19937 rng_;
    std::normal_distribution<double> noise_;
    double ambient_temp_;
    double heating_power_;
    bool heating_active_;

public:
    TemperatureSensor(double initial_temp = 18.0, double ambient = 15.0)
        : current_temp_(initial_temp)
        , target_temp_(21.0)
        , rng_(std::random_device{}())
        , noise_(0.0, 0.3)  // ±0.3°C sensor noise
        , ambient_temp_(ambient)
        , heating_power_(2.5)  // °C per second when heating
        , heating_active_(false)
    {}

    void setTarget(double target) { target_temp_ = target; }
    void setHeating(bool active) { heating_active_ = active; }

    // Simulate one time step (dt seconds)
    double step(double dt) {
        // Natural cooling toward ambient (slower cooling)
        double cooling = (current_temp_ - ambient_temp_) * 0.02 * dt;
        current_temp_ -= cooling;

        // Heating effect
        if (heating_active_) {
            current_temp_ += heating_power_ * dt;
        }

        // Add sensor noise
        return current_temp_ + noise_(rng_);
    }

    double getCurrentTemp() const { return current_temp_; }
};

// Hysteresis heating controller - observes boolean hysteresis output
class HysteresisHeatingController : public IObserver<bool> {
private:
    bool heating_on_;
    std::function<void(bool)> on_change_;

public:
    HysteresisHeatingController(std::function<void(bool)> callback = nullptr)
        : heating_on_(false), on_change_(callback) {}

    void OnNext(const bool& heating_state) override {
        if (heating_state != heating_on_) {
            heating_on_ = heating_state;
            if (on_change_) on_change_(heating_on_);
            Serial.print("[HEATING] "); 
            Serial.println(heating_on_ ? "ON" : "OFF");
        }
    }

    void OnCompleted() override {
        Serial.println("[HEATING] Controller completed");
    }

    void OnError(const std::exception& e) override {
        Serial.print("[HEATING] Error: "); 
        Serial.println(e.what());
    }

    bool isHeating() const { return heating_on_; }
};

// Global objects for Arduino setup/loop
std::shared_ptr<Subject<double>> temp_subject;
std::shared_ptr<IObservable<double>> temp_observable;
std::shared_ptr<IObservable<double>> pid_output;
std::shared_ptr<IObservable<bool>> hysteresis_output;
std::shared_ptr<SimpleLogger<double>> temp_logger;
std::shared_ptr<SimpleLogger<double>> pid_logger;
std::shared_ptr<SimpleLogger<bool>> hysteresis_logger;
std::shared_ptr<Subscription> heating_sub;
std::shared_ptr<Subscription> temp_sub;
std::shared_ptr<Subscription> pid_sub;
std::shared_ptr<Subscription> hysteresis_sub;

TemperatureSensor sensor(18.0, 15.0);
HysteresisHeatingController heating_ctrl;

const double dt = 1.0;
int step_count = 0;

void setup() {
    Serial.begin(115200);
    while (!Serial) delay(10);
    
    Serial.println("=== Temperature PID Heating Control Demo (Fluent API) ===");
    Serial.println("Target: 21.0°C | PID: Kp=2.0, Ki=0.1, Kd=0.5 | dt=1.0s");
    Serial.println("Hysteresis: threshold=0.5, band=0.15 (ON at 0.65, OFF at 0.35)");
    Serial.println("----------------------------------------");

    sensor.setTarget(21.0);

    // Create a subject to feed temperature readings
    temp_subject = std::make_shared<Subject<double>>();
    temp_observable = std::make_shared<IObservable<double>>(temp_subject);

    // PID parameters - tuned for the simulation
    const double setpoint = 21.0;
    const double kp = 2.0;
    const double ki = 0.1;
    const double kd = 0.5;
    const double min_output = 0.0;
    const double max_output = 1.0;

    // Hysteresis parameters - prevent rapid switching around 0.5 threshold
    const double hysteresis_threshold = 0.5;
    const double hysteresis_band = 0.15;  // Switch ON at 0.65, OFF at 0.35

    // Create PID controller (FLUENT STYLE)
    pid_output = std::make_shared<IObservable<double>>(temp_observable->PID(setpoint, kp, ki, kd, dt, min_output, max_output));

    // Apply hysteresis to prevent rapid ON/OFF switching (fluent)
    hysteresis_output = std::make_shared<IObservable<bool>>(pid_output->Hysteresis(hysteresis_threshold, hysteresis_band));

    // Create heating controller (binary output) - observes hysteresis output (bool)
    heating_ctrl = HysteresisHeatingController([&](bool on) {
        sensor.setHeating(on);
    });

    // Subscribe heating controller to hysteresis output (fluent, store subscription)
    heating_sub = hysteresis_output->Subscribe(std::make_shared<HysteresisHeatingController>(heating_ctrl));

    // Also log temperature and PID output for monitoring (store subscriptions)
    temp_logger = std::make_shared<SimpleLogger<double>>();
    pid_logger = std::make_shared<SimpleLogger<double>>();
    hysteresis_logger = std::make_shared<SimpleLogger<bool>>();

    temp_sub = temp_subject->Subscribe(temp_logger);
    pid_sub = pid_output->Subscribe(pid_logger);
    hysteresis_sub = hysteresis_output->Subscribe(hysteresis_logger);
}

void loop() {
    if (step_count >= 60) {
        temp_subject->OnCompleted();
        Serial.println("----------------------------------------");
        Serial.println("Simulation complete.");
        Serial.print("Final temperature: "); 
        Serial.print(sensor.getCurrentTemp()); 
        Serial.println("°C");
        // In real Arduino, you'd stop here or enter deep sleep
        while (true) {
            delay(1000);
        }
    }

    double reading = sensor.step(dt);
    temp_subject->OnNext(reading);

    // Print status every 5 seconds
    if (step_count % 5 == 0) {
        Serial.print("t="); 
        Serial.print(step_count); 
        Serial.print("s | Temp: "); 
        Serial.print(reading);
        Serial.print("°C | PID: "); 
        Serial.print(pid_logger->GetLastValue());
        Serial.print(" | Hysteresis: "); 
        Serial.print(hysteresis_logger->GetLastValue() ? "ON" : "OFF");
        Serial.print(" | Heat: "); 
        Serial.println(heating_ctrl.isHeating() ? "ON" : "OFF");
    }

    step_count++;
    delay(100);  // Speed up demo
}
