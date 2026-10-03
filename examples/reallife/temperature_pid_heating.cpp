/**
 * Real-life Example: Temperature PID Heating Control
 * 
 * Simulates a temperature sensor feeding a PID controller that outputs
 * a binary heating signal (ON/OFF) based on the PID output threshold.
 * With hysteresis to prevent rapid switching.
 * 
 * Hardware: ESP32/Arduino compatible
 * Library: Micro-Reactive
 */

#include <micro-reactive.h>
#include <iostream>
#include <chrono>
#include <thread>
#include <random>
#include <cmath>
#include <memory>

using namespace rx;

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
            std::cout << "[HEATING] " << (heating_on_ ? "ON" : "OFF") << std::endl;
        }
    }

    void OnCompleted() override {
        std::cout << "[HEATING] Controller completed" << std::endl;
    }

    void OnError(const std::exception& e) override {
        std::cerr << "[HEATING] Error: " << e.what() << std::endl;
    }

    bool isHeating() const { return heating_on_; }
};  // <-- missing semicolon;

// Global objects for Arduino setup/loop
std::shared_ptr<Subject<double>> temp_subject;
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
    std::cout << "=== Temperature PID Heating Control Demo ===" << std::endl;
    std::cout << "Target: 21.0°C | PID: Kp=2.0, Ki=0.1, Kd=0.5 | dt=1.0s" << std::endl;
    std::cout << "Hysteresis: threshold=0.5, band=0.15 (ON at 0.65, OFF at 0.35)" << std::endl;
    std::cout << "----------------------------------------" << std::endl;

    sensor.setTarget(21.0);

    // Create a subject to feed temperature readings
    temp_subject = std::make_shared<Subject<double>>();

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

    // Create PID controller (traditional style)
    pid_output = PID<double>(temp_subject, setpoint, kp, ki, kd, dt, min_output, max_output);

    // Apply hysteresis to prevent rapid ON/OFF switching
    hysteresis_output = Hysteresis<double>(pid_output, hysteresis_threshold, hysteresis_band);

    // Create heating controller (binary output) - observes hysteresis output (bool)
    heating_ctrl = HysteresisHeatingController([&](bool on) {
        sensor.setHeating(on);
    });

    // Subscribe heating controller to hysteresis output (store subscription)
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
        std::cout << "----------------------------------------" << std::endl;
        std::cout << "Simulation complete." << std::endl;
        std::cout << "Final temperature: " << sensor.getCurrentTemp() << "°C" << std::endl;
        // In real Arduino, you'd stop here or enter deep sleep
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    double reading = sensor.step(dt);
    temp_subject->OnNext(reading);

    // Print status every 5 seconds
    if (step_count % 5 == 0) {
        std::cout << "t=" << step_count << "s | Temp: " << reading
                  << "°C | PID: " << pid_logger->GetLastValue()
                  << " | Hysteresis: " << (hysteresis_logger->GetLastValue() ? "ON" : "OFF")
                  << " | Heat: " << (heating_ctrl.isHeating() ? "ON" : "OFF") << std::endl;
    }

    step_count++;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));  // Speed up demo
}

// For native testing, provide a main that calls setup/loop
#ifndef ARDUINO
int main() {
    setup();
    while (true) {
        loop();
    }
    return 0;
}
#endif